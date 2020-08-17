package autoconf

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/agent/cache"
	cachetype "github.com/hashicorp/consul/agent/cache-types"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/connect"
	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/token"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/proto/pbautoconf"
	"github.com/hashicorp/consul/proto/pbconfig"
	"github.com/hashicorp/consul/sdk/testutil"
	"github.com/hashicorp/consul/sdk/testutil/retry"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type configLoader struct {
	opts config.BuilderOpts
}

func (c *configLoader) Load(source config.Source) (*config.RuntimeConfig, []string, error) {
	return config.Load(c.opts, source)
}

func (c *configLoader) addConfigHCL(cfg string) {
	c.opts.HCL = append(c.opts.HCL, cfg)
}

func requireChanNotReady(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		require.Fail(t, "chan is ready when it shouldn't be")
	default:
		return
	}
}

func requireChanReady(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
		return
	default:
		require.Fail(t, "chan is not ready when it should be")
	}
}

func waitForChan(timer *time.Timer, ch <-chan struct{}) bool {
	select {
	case <-timer.C:
		return false
	case <-ch:
		return true
	}
}

func waitForChans(timeout time.Duration, chans ...<-chan struct{}) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for _, ch := range chans {
		if !waitForChan(timer, ch) {
			return false
		}
	}
	return true
}

func TestNew(t *testing.T) {
	type testCase struct {
		modify   func(*Config)
		err      string
		validate func(t *testing.T, ac *AutoConfig)
	}

	cases := map[string]testCase{
		"no-direct-rpc": {
			modify: func(c *Config) {
				c.DirectRPC = nil
			},
			err: "must provide a direct RPC delegate",
		},
		"no-config-loader": {
			modify: func(c *Config) {
				c.Loader = nil
			},
			err: "must provide a config loader",
		},
		"no-cache": {
			modify: func(c *Config) {
				c.Cache = nil
			},
			err: "must provide a cache",
		},
		"no-tls-configurator": {
			modify: func(c *Config) {
				c.TLSConfigurator = nil
			},
			err: "must provide a TLS configurator",
		},
		"no-tokens": {
			modify: func(c *Config) {
				c.Tokens = nil
			},
			err: "must provide a token store",
		},
		"ok": {
			validate: func(t *testing.T, ac *AutoConfig) {
				t.Helper()
				require.NotNil(t, ac.logger)
				require.NotNil(t, ac.acConfig.Waiter)
				require.Equal(t, time.Minute, ac.acConfig.FallbackRetry)
				require.Equal(t, 10*time.Second, ac.acConfig.FallbackLeeway)
			},
		},
	}

	for name, tcase := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := Config{
				Loader: func(source config.Source) (cfg *config.RuntimeConfig, warnings []string, err error) {
					return nil, nil, nil
				},
				DirectRPC:       newMockDirectRPC(t),
				Tokens:          newMockTokenStore(t),
				Cache:           newMockCache(t),
				TLSConfigurator: newMockTLSConfigurator(t),
				ServerProvider:  newMockServerProvider(t),
			}

			if tcase.modify != nil {
				tcase.modify(&cfg)
			}

			ac, err := New(cfg)
			if tcase.err != "" {
				testutil.RequireErrorContains(t, err, tcase.err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, ac)
				if tcase.validate != nil {
					tcase.validate(t, ac)
				}
			}
		})
	}
}

func TestReadConfig(t *testing.T) {
	// just testing that some auto config source gets injected
	ac := AutoConfig{
		autoConfigSource: config.LiteralSource{
			Name:   autoConfigFileName,
			Config: config.Config{NodeName: stringPointer("hobbiton")},
		},
		logger: testutil.Logger(t),
		acConfig: Config{
			Loader: func(source config.Source) (*config.RuntimeConfig, []string, error) {
				cfg, _, err := source.Parse()
				if err != nil {
					return nil, nil, err
				}
				return &config.RuntimeConfig{
					DevMode:  true,
					NodeName: *cfg.NodeName,
				}, nil, nil
			},
		},
	}

	cfg, err := ac.ReadConfig()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "hobbiton", cfg.NodeName)
	require.True(t, cfg.DevMode)
	require.Same(t, ac.config, cfg)
}

func setupRuntimeConfig(t *testing.T) *configLoader {
	t.Helper()

	dataDir := testutil.TempDir(t, "auto-config")
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	opts := config.BuilderOpts{
		Config: config.Config{
			DataDir:    &dataDir,
			Datacenter: stringPointer("dc1"),
			NodeName:   stringPointer("autoconf"),
			BindAddr:   stringPointer("127.0.0.1"),
		},
	}
	return &configLoader{opts: opts}
}

func TestInitialConfiguration_disabled(t *testing.T) {
	loader := setupRuntimeConfig(t)
	loader.addConfigHCL(`
		primary_datacenter = "primary"
		auto_config = {
			enabled = false
		}
	`)

	conf := newMockedConfig(t).Config
	conf.Loader = loader.Load

	ac, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, ac)

	cfg, err := ac.InitialConfiguration(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "primary", cfg.PrimaryDatacenter)
	require.NoFileExists(t, filepath.Join(*loader.opts.Config.DataDir, autoConfigFileName))
}

func TestInitialConfiguration_cancelled(t *testing.T) {
	mcfg := newMockedConfig(t)

	loader := setupRuntimeConfig(t)
	loader.addConfigHCL(`
		primary_datacenter = "primary"
		auto_config = {
			enabled = true
			intro_token = "blarg"
			server_addresses = ["127.0.0.1:8300"]
		}
		verify_outgoing = true
	`)
	mcfg.Config.Loader = loader.Load

	expectedRequest := pbautoconf.AutoConfigRequest{
		Datacenter: "dc1",
		Node:       "autoconf",
		JWT:        "blarg",
	}

	mcfg.directRPC.On("RPC", "dc1", "autoconf", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8300}, "AutoConfig.InitialConfiguration", &expectedRequest, mock.Anything).Return(fmt.Errorf("injected error")).Times(0)
	mcfg.serverProvider.On("FindLocalServer").Return(nil).Times(0)

	ac, err := New(mcfg.Config)
	require.NoError(t, err)
	require.NotNil(t, ac)

	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(100*time.Millisecond))
	defer cancelFn()

	cfg, err := ac.InitialConfiguration(ctx)
	testutil.RequireErrorContains(t, err, context.DeadlineExceeded.Error())
	require.Nil(t, cfg)
}

func TestInitialConfiguration_restored(t *testing.T) {
	mcfg := newMockedConfig(t)

	loader := setupRuntimeConfig(t)
	loader.addConfigHCL(`
		auto_config = {
			enabled = true
			intro_token ="blarg"
			server_addresses = ["127.0.0.1:8300"]
		}
		verify_outgoing = true
	`)

	mcfg.Config.Loader = loader.Load

	_, indexedRoots, cert, extraCACerts := mcfg.setupInitialTLS(t, "autoconf", "dc1", "secret")

	// persist an auto config response to the data dir where it is expected
	persistedFile := filepath.Join(*loader.opts.Config.DataDir, autoConfigFileName)
	response := &pbautoconf.AutoConfigResponse{
		Config: &pbconfig.Config{
			PrimaryDatacenter: "primary",
			TLS: &pbconfig.TLS{
				VerifyServerHostname: true,
			},
		},
		CARoots:             mustTranslateCARootsToProtobuf(t, indexedRoots),
		Certificate:         mustTranslateIssuedCertToProtobuf(t, cert),
		ExtraCACertificates: extraCACerts,
	}
	data, err := pbMarshaler.MarshalToString(response)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(persistedFile, []byte(data), 0600))

	// prepopulation is going to grab the token to populate the correct cache key
	mcfg.tokens.On("AgentToken").Return("secret").Times(0)

	ac, err := New(mcfg.Config)
	require.NoError(t, err)
	require.NotNil(t, ac)

	cfg, err := ac.InitialConfiguration(context.Background())
	require.NoError(t, err, data)
	require.NotNil(t, cfg)
	require.Equal(t, "primary", cfg.PrimaryDatacenter)
}

func TestInitialConfiguration_success(t *testing.T) {
	mcfg := newMockedConfig(t)
	loader := setupRuntimeConfig(t)
	loader.addConfigHCL(`
		auto_config = {
			enabled = true
			intro_token ="blarg"
			server_addresses = ["127.0.0.1:8300"]
		}
		verify_outgoing = true
	`)
	mcfg.Config.Loader = loader.Load

	_, indexedRoots, cert, extraCerts := mcfg.setupInitialTLS(t, "autoconf", "dc1", "secret")

	// prepopulation is going to grab the token to populate the correct cache key
	mcfg.tokens.On("AgentToken").Return("secret").Times(0)

	// no server provider
	mcfg.serverProvider.On("FindLocalServer").Return(nil).Times(0)

	populateResponse := func(args mock.Arguments) {
		resp, ok := args.Get(5).(*pbautoconf.AutoConfigResponse)
		require.True(t, ok)
		resp.Config = &pbconfig.Config{
			PrimaryDatacenter: "primary",
			TLS: &pbconfig.TLS{
				VerifyServerHostname: true,
			},
		}

		resp.CARoots = mustTranslateCARootsToProtobuf(t, indexedRoots)
		resp.Certificate = mustTranslateIssuedCertToProtobuf(t, cert)
		resp.ExtraCACertificates = extraCerts
	}

	expectedRequest := pbautoconf.AutoConfigRequest{
		Datacenter: "dc1",
		Node:       "autoconf",
		JWT:        "blarg",
	}

	mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8300},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(nil).Run(populateResponse)

	ac, err := New(mcfg.Config)
	require.NoError(t, err)
	require.NotNil(t, ac)

	cfg, err := ac.InitialConfiguration(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "primary", cfg.PrimaryDatacenter)

	// the file was written to.
	persistedFile := filepath.Join(*loader.opts.Config.DataDir, autoConfigFileName)
	require.FileExists(t, persistedFile)
}

func TestInitialConfiguration_retries(t *testing.T) {
	mcfg := newMockedConfig(t)
	loader := setupRuntimeConfig(t)
	loader.addConfigHCL(`
		auto_config = {
			enabled = true
			intro_token ="blarg"
			server_addresses = [
				"198.18.0.1:8300",
				"198.18.0.2:8398",
				"198.18.0.3:8399",
				"127.0.0.1:1234"
			]
		}
		verify_outgoing = true
	`)
	mcfg.Config.Loader = loader.Load

	// reduce the retry wait times to make this test run faster
	mcfg.Config.Waiter = lib.NewRetryWaiter(2, 0, 1*time.Millisecond, nil)

	_, indexedRoots, cert, extraCerts := mcfg.setupInitialTLS(t, "autoconf", "dc1", "secret")

	// prepopulation is going to grab the token to populate the correct cache key
	mcfg.tokens.On("AgentToken").Return("secret").Times(0)

	// no server provider
	mcfg.serverProvider.On("FindLocalServer").Return(nil).Times(0)

	populateResponse := func(args mock.Arguments) {
		resp, ok := args.Get(5).(*pbautoconf.AutoConfigResponse)
		require.True(t, ok)
		resp.Config = &pbconfig.Config{
			PrimaryDatacenter: "primary",
			TLS: &pbconfig.TLS{
				VerifyServerHostname: true,
			},
		}

		resp.CARoots = mustTranslateCARootsToProtobuf(t, indexedRoots)
		resp.Certificate = mustTranslateIssuedCertToProtobuf(t, cert)
		resp.ExtraCACertificates = extraCerts
	}

	expectedRequest := pbautoconf.AutoConfigRequest{
		Datacenter: "dc1",
		Node:       "autoconf",
		JWT:        "blarg",
	}

	// basically the 198.18.0.* addresses should fail indefinitely. the first time through the
	// outer loop we inject a failure for the DNS resolution of localhost to 127.0.0.1. Then
	// the second time through the outer loop we allow the localhost one to work.
	mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(198, 18, 0, 1), Port: 8300},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(fmt.Errorf("injected failure")).Times(0)
	mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(198, 18, 0, 2), Port: 8398},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(fmt.Errorf("injected failure")).Times(0)
	mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(198, 18, 0, 3), Port: 8399},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(fmt.Errorf("injected failure")).Times(0)
	mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(fmt.Errorf("injected failure")).Once()
	mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(nil).Run(populateResponse).Once()

	ac, err := New(mcfg.Config)
	require.NoError(t, err)
	require.NotNil(t, ac)

	cfg, err := ac.InitialConfiguration(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "primary", cfg.PrimaryDatacenter)

	// the file was written to.
	persistedFile := filepath.Join(*loader.opts.Config.DataDir, autoConfigFileName)
	require.FileExists(t, persistedFile)
}

func TestGoRoutineManagement(t *testing.T) {
	mcfg := newMockedConfig(t)
	loader := setupRuntimeConfig(t)
	loader.addConfigHCL(`
		auto_config = {
			enabled = true
			intro_token ="blarg"
			server_addresses = ["127.0.0.1:8300"]
		}
		verify_outgoing = true
	`)
	mcfg.Config.Loader = loader.Load

	// prepopulation is going to grab the token to populate the correct cache key
	mcfg.tokens.On("AgentToken").Return("secret").Times(0)

	ac, err := New(mcfg.Config)
	require.NoError(t, err)

	// priming the config so some other requests will work properly that need to
	// read from the configuration. We are going to avoid doing InitialConfiguration
	// for this test as we only are really concerned with the go routine management
	_, err = ac.ReadConfig()
	require.NoError(t, err)

	var rootsCtx context.Context
	var leafCtx context.Context
	var ctxLock sync.Mutex

	rootsReq := ac.caRootsRequest()
	mcfg.cache.On("Notify",
		mock.Anything,
		cachetype.ConnectCARootName,
		&rootsReq,
		rootsWatchID,
		mock.Anything,
	).Return(nil).Times(2).Run(func(args mock.Arguments) {
		ctxLock.Lock()
		rootsCtx = args.Get(0).(context.Context)
		ctxLock.Unlock()
	})

	leafReq := ac.leafCertRequest()
	mcfg.cache.On("Notify",
		mock.Anything,
		cachetype.ConnectCALeafName,
		&leafReq,
		leafWatchID,
		mock.Anything,
	).Return(nil).Times(2).Run(func(args mock.Arguments) {
		ctxLock.Lock()
		leafCtx = args.Get(0).(context.Context)
		ctxLock.Unlock()
	})

	// we will start/stop things twice
	mcfg.tokens.On("Notify", token.TokenKindAgent).Return(token.Notifier{}).Times(2)
	mcfg.tokens.On("StopNotify", token.Notifier{}).Times(2)

	mcfg.tlsCfg.On("AutoEncryptCertNotAfter").Return(time.Now().Add(10 * time.Minute)).Times(0)

	// ensure that auto-config isn't running
	require.False(t, ac.IsRunning())

	// ensure that nothing bad happens and that it reports as stopped
	require.False(t, ac.Stop())

	// ensure that the Done chan also reports that things are not running
	// in other words the chan is immediately selectable
	requireChanReady(t, ac.Done())

	// start auto-config
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, ac.Start(ctx))

	waitForContexts := func() bool {
		ctxLock.Lock()
		defer ctxLock.Unlock()
		return !(rootsCtx == nil || leafCtx == nil)
	}

	// wait for the cache notifications to get started
	require.Eventually(t, waitForContexts, 100*time.Millisecond, 10*time.Millisecond)

	// hold onto the Done chan to test for the go routine exiting
	done := ac.Done()

	// ensure we report as running
	require.True(t, ac.IsRunning())

	// ensure the done chan is not selectable yet
	requireChanNotReady(t, done)

	// ensure we error if we attempt to start again
	err = ac.Start(ctx)
	testutil.RequireErrorContains(t, err, "AutoConfig is already running")

	// now stop things - it should return true indicating that it was running
	// when we attempted to stop it.
	require.True(t, ac.Stop())

	// ensure that the go routine shuts down - it will close the done chan. Also it should cancel
	// the cache watches by cancelling the context it passed into the Notify call.
	require.True(t, waitForChans(100*time.Millisecond, done, leafCtx.Done(), rootsCtx.Done()), "AutoConfig didn't shut down")
	require.False(t, ac.IsRunning())

	// restart it
	require.NoError(t, ac.Start(ctx))

	// get the new Done chan
	done = ac.Done()

	// ensure that context cancellation causes us to stop as well
	cancel()
	require.True(t, waitForChans(100*time.Millisecond, done))
}

type testAutoConfig struct {
	mcfg          *mockedConfig
	ac            *AutoConfig
	tokenUpdates  chan struct{}
	originalToken string

	initialRoots *structs.IndexedCARoots
	initialCert  *structs.IssuedCert
	extraCerts   []string
}

func startedAutoConfig(t *testing.T) testAutoConfig {
	t.Helper()
	mcfg := newMockedConfig(t)
	loader := setupRuntimeConfig(t)
	loader.addConfigHCL(`
		auto_config = {
			enabled = true
			intro_token ="blarg"
			server_addresses = ["127.0.0.1:8300"]
		}
		verify_outgoing = true
	`)
	mcfg.Config.Loader = loader.Load
	mcfg.Config.FallbackLeeway = time.Nanosecond

	originalToken := "a5deaa25-11ca-48bf-a979-4c3a7aa4b9a9"

	// we expect this to be retrieved twice: first during cache prepopulation
	// and then again when setting up the cache watch for the leaf cert.
	// However one of those expectations is setup in the expectInitialTLS
	// method so we only need one more here
	mcfg.tokens.On("AgentToken").Return(originalToken).Once()

	// this is called once during Start to initialze the token watches
	tokenUpdateCh := make(chan struct{})
	tokenNotifier := token.Notifier{
		Ch: tokenUpdateCh,
	}
	mcfg.tokens.On("Notify", token.TokenKindAgent).Once().Return(tokenNotifier)
	mcfg.tokens.On("StopNotify", tokenNotifier).Once()

	// expect the roots watch on the cache
	mcfg.cache.On("Notify",
		mock.Anything,
		cachetype.ConnectCARootName,
		&structs.DCSpecificRequest{Datacenter: "dc1"},
		rootsWatchID,
		mock.Anything,
	).Return(nil).Once()

	mcfg.cache.On("Notify",
		mock.Anything,
		cachetype.ConnectCALeafName,
		&cachetype.ConnectCALeafRequest{
			Datacenter: "dc1",
			Agent:      "autoconf",
			Token:      originalToken,
		},
		leafWatchID,
		mock.Anything,
	).Return(nil).Once()

	// override the server provider - most of the other tests set it up so that this
	// always returns no server (simulating a state where we haven't joined gossip).
	// this seems like a good place to ensure this other way of finding server information
	// works
	mcfg.serverProvider.On("FindLocalServer").Once().Return(&metadata.Server{
		Addr: &net.TCPAddr{IP: net.IPv4(198, 18, 0, 1), Port: 8300},
	})

	_, indexedRoots, cert, extraCerts := mcfg.setupInitialTLS(t, "autoconf", "dc1", originalToken)

	mcfg.tlsCfg.On("AutoEncryptCertNotAfter").Return(cert.ValidBefore).Once()

	populateResponse := func(args mock.Arguments) {
		resp, ok := args.Get(5).(*pbautoconf.AutoConfigResponse)
		require.True(t, ok)
		resp.Config = &pbconfig.Config{
			PrimaryDatacenter: "primary",
			TLS: &pbconfig.TLS{
				VerifyServerHostname: true,
			},
		}

		resp.CARoots = mustTranslateCARootsToProtobuf(t, indexedRoots)
		resp.Certificate = mustTranslateIssuedCertToProtobuf(t, cert)
		resp.ExtraCACertificates = extraCerts
	}

	expectedRequest := pbautoconf.AutoConfigRequest{
		Datacenter: "dc1",
		Node:       "autoconf",
		JWT:        "blarg",
	}

	mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(198, 18, 0, 1), Port: 8300},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(nil).Run(populateResponse).Once()

	ac, err := New(mcfg.Config)
	require.NoError(t, err)
	require.NotNil(t, ac)

	cfg, err := ac.InitialConfiguration(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.True(t, cfg.VerifyServerHostname)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, ac.Start(ctx))
	t.Cleanup(func() {
		done := ac.Done()
		cancel()
		timer := time.NewTimer(1 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
			// do nothing
		case <-timer.C:
			t.Fatalf("AutoConfig wasn't stopped within 1 second after test completion")
		}
	})

	return testAutoConfig{
		mcfg:          mcfg,
		ac:            ac,
		tokenUpdates:  tokenUpdateCh,
		originalToken: originalToken,
		initialRoots:  indexedRoots,
		initialCert:   cert,
		extraCerts:    extraCerts,
	}
}

// this test ensures that the cache watches are restarted with
// the updated token after receiving a token update
func TestTokenUpdate(t *testing.T) {
	testAC := startedAutoConfig(t)

	newToken := "1a4cc445-86ed-46b4-a355-bbf5a11dddb0"

	rootsCtx, rootsCancel := context.WithCancel(context.Background())
	testAC.mcfg.cache.On("Notify",
		mock.Anything,
		cachetype.ConnectCARootName,
		&structs.DCSpecificRequest{Datacenter: testAC.ac.config.Datacenter},
		rootsWatchID,
		mock.Anything,
	).Return(nil).Once().Run(func(args mock.Arguments) {
		rootsCancel()
	})

	leafCtx, leafCancel := context.WithCancel(context.Background())
	testAC.mcfg.cache.On("Notify",
		mock.Anything,
		cachetype.ConnectCALeafName,
		&cachetype.ConnectCALeafRequest{
			Datacenter: "dc1",
			Agent:      "autoconf",
			Token:      newToken,
		},
		leafWatchID,
		mock.Anything,
	).Return(nil).Once().Run(func(args mock.Arguments) {
		leafCancel()
	})

	// this will be retrieved once when resetting the leaf cert watch
	testAC.mcfg.tokens.On("AgentToken").Return(newToken).Once()

	// send the notification about the token update
	testAC.tokenUpdates <- struct{}{}

	// wait for the leaf cert watches
	require.True(t, waitForChans(100*time.Millisecond, leafCtx.Done(), rootsCtx.Done()), "New cache watches were not started within 100ms")
}

func TestRootsUpdate(t *testing.T) {
	testAC := startedAutoConfig(t)

	secondCA := connect.TestCA(t, testAC.initialRoots.Roots[0])
	secondRoots := structs.IndexedCARoots{
		ActiveRootID: secondCA.ID,
		TrustDomain:  connect.TestClusterID,
		Roots: []*structs.CARoot{
			secondCA,
			testAC.initialRoots.Roots[0],
		},
		QueryMeta: structs.QueryMeta{
			Index: 99,
		},
	}

	updatedCtx, cancel := context.WithCancel(context.Background())
	testAC.mcfg.tlsCfg.On("UpdateAutoTLS",
		testAC.extraCerts,
		[]string{secondCA.RootCert, testAC.initialRoots.Roots[0].RootCert},
		testAC.initialCert.CertPEM,
		"redacted",
		true,
	).Return(nil).Once().Run(func(args mock.Arguments) {
		cancel()
	})

	// when a cache event comes in we end up recalculating the fallback timer which requires this call
	testAC.mcfg.tlsCfg.On("AutoEncryptCertNotAfter").Return(time.Now().Add(10 * time.Minute)).Once()

	req := structs.DCSpecificRequest{Datacenter: "dc1"}
	require.True(t, testAC.mcfg.cache.sendNotification(context.Background(), req.CacheInfo().Key, cache.UpdateEvent{
		CorrelationID: rootsWatchID,
		Result:        &secondRoots,
		Meta: cache.ResultMeta{
			Index: secondRoots.Index,
		},
	}))

	require.True(t, waitForChans(100*time.Millisecond, updatedCtx.Done()), "TLS certificates were not updated within the alotted time")

	// persisting these to disk happens right after the chan we are waiting for will have fired above
	// however there is no deterministic way to know once its been written outside of maybe a filesystem
	// event notifier. That seems a little heavy handed just for this and especially to do in any sort
	// of cross platform way.
	retry.Run(t, func(r *retry.R) {
		resp, err := testAC.ac.readPersistedAutoConfig()
		require.NoError(r, err)
		require.Equal(r, secondRoots.ActiveRootID, resp.CARoots.GetActiveRootID())
	})
}

func TestCertUpdate(t *testing.T) {
	testAC := startedAutoConfig(t)
	secondCert := newLeaf(t, "autoconf", "dc1", testAC.initialRoots.Roots[0], 99, 10*time.Minute)

	updatedCtx, cancel := context.WithCancel(context.Background())
	testAC.mcfg.tlsCfg.On("UpdateAutoTLS",
		testAC.extraCerts,
		[]string{testAC.initialRoots.Roots[0].RootCert},
		secondCert.CertPEM,
		"redacted",
		true,
	).Return(nil).Once().Run(func(args mock.Arguments) {
		cancel()
	})

	// when a cache event comes in we end up recalculating the fallback timer which requires this call
	testAC.mcfg.tlsCfg.On("AutoEncryptCertNotAfter").Return(secondCert.ValidBefore).Once()

	req := cachetype.ConnectCALeafRequest{
		Datacenter: "dc1",
		Agent:      "autoconf",
		Token:      testAC.originalToken,
	}
	require.True(t, testAC.mcfg.cache.sendNotification(context.Background(), req.CacheInfo().Key, cache.UpdateEvent{
		CorrelationID: leafWatchID,
		Result:        secondCert,
		Meta: cache.ResultMeta{
			Index: secondCert.ModifyIndex,
		},
	}))

	require.True(t, waitForChans(100*time.Millisecond, updatedCtx.Done()), "TLS certificates were not updated within the alotted time")

	// persisting these to disk happens after all the things we would wait for in assertCertUpdated
	// will have fired. There is no deterministic way to know once its been written so we wrap
	// this in a retry.
	retry.Run(t, func(r *retry.R) {
		resp, err := testAC.ac.readPersistedAutoConfig()
		require.NoError(r, err)

		// ensure the roots got persisted to disk
		require.Equal(r, secondCert.CertPEM, resp.Certificate.GetCertPEM())
	})
}

func TestFallback(t *testing.T) {
	testAC := startedAutoConfig(t)

	// at this point everything is operating normally and we are just
	// waiting for events. We are going to send a new cert that is basically
	// already expired and then allow the fallback routine to kick in.
	secondCert := newLeaf(t, "autoconf", "dc1", testAC.initialRoots.Roots[0], 100, time.Nanosecond)
	secondCA := connect.TestCA(t, testAC.initialRoots.Roots[0])
	secondRoots := structs.IndexedCARoots{
		ActiveRootID: secondCA.ID,
		TrustDomain:  connect.TestClusterID,
		Roots: []*structs.CARoot{
			secondCA,
			testAC.initialRoots.Roots[0],
		},
		QueryMeta: structs.QueryMeta{
			Index: 101,
		},
	}
	thirdCert := newLeaf(t, "autoconf", "dc1", secondCA, 102, 10*time.Minute)

	// setup the expectation for when the certs got updated initially
	updatedCtx, updateCancel := context.WithCancel(context.Background())
	testAC.mcfg.tlsCfg.On("UpdateAutoTLS",
		testAC.extraCerts,
		[]string{testAC.initialRoots.Roots[0].RootCert},
		secondCert.CertPEM,
		"redacted",
		true,
	).Return(nil).Once().Run(func(args mock.Arguments) {
		updateCancel()
	})

	// when a cache event comes in we end up recalculating the fallback timer which requires this call
	testAC.mcfg.tlsCfg.On("AutoEncryptCertNotAfter").Return(secondCert.ValidBefore).Once()
	testAC.mcfg.tlsCfg.On("AutoEncryptCertExpired").Return(true).Once()

	fallbackCtx, fallbackCancel := context.WithCancel(context.Background())

	// also testing here that we can change server IPs for ongoing operations
	testAC.mcfg.serverProvider.On("FindLocalServer").Once().Return(&metadata.Server{
		Addr: &net.TCPAddr{IP: net.IPv4(198, 18, 23, 2), Port: 8300},
	})

	// after sending the notification for the cert update another InitialConfiguration RPC
	// will be made to pull down the latest configuration. So we need to set up the response
	// for the second RPC
	populateResponse := func(args mock.Arguments) {
		resp, ok := args.Get(5).(*pbautoconf.AutoConfigResponse)
		require.True(t, ok)
		resp.Config = &pbconfig.Config{
			PrimaryDatacenter: "primary",
			TLS: &pbconfig.TLS{
				VerifyServerHostname: true,
			},
		}

		resp.CARoots = mustTranslateCARootsToProtobuf(t, &secondRoots)
		resp.Certificate = mustTranslateIssuedCertToProtobuf(t, thirdCert)
		resp.ExtraCACertificates = testAC.extraCerts

		fallbackCancel()
	}

	expectedRequest := pbautoconf.AutoConfigRequest{
		Datacenter: "dc1",
		Node:       "autoconf",
		JWT:        "blarg",
	}

	testAC.mcfg.directRPC.On(
		"RPC",
		"dc1",
		"autoconf",
		&net.TCPAddr{IP: net.IPv4(198, 18, 23, 2), Port: 8300},
		"AutoConfig.InitialConfiguration",
		&expectedRequest,
		&pbautoconf.AutoConfigResponse{}).Return(nil).Run(populateResponse).Once()

	testAC.mcfg.expectInitialTLS(t, "autoconf", "dc1", testAC.originalToken, secondCA, &secondRoots, thirdCert, testAC.extraCerts)

	// after the second RPC we now will use the new certs validity period in the next run loop iteration
	testAC.mcfg.tlsCfg.On("AutoEncryptCertNotAfter").Return(time.Now().Add(10 * time.Minute)).Once()

	// now that all the mocks are set up we can trigger the whole thing by sending the second expired cert
	// as a cache update event.
	req := cachetype.ConnectCALeafRequest{
		Datacenter: "dc1",
		Agent:      "autoconf",
		Token:      testAC.originalToken,
	}
	require.True(t, testAC.mcfg.cache.sendNotification(context.Background(), req.CacheInfo().Key, cache.UpdateEvent{
		CorrelationID: leafWatchID,
		Result:        secondCert,
		Meta: cache.ResultMeta{
			Index: secondCert.ModifyIndex,
		},
	}))

	// wait for the TLS certificates to get updated
	require.True(t, waitForChans(100*time.Millisecond, updatedCtx.Done()), "TLS certificates were not updated within the alotted time")

	// now wait for the fallback routine to be invoked
	require.True(t, waitForChans(100*time.Millisecond, fallbackCtx.Done()), "fallback routines did not get invoked within the alotted time")

	// persisting these to disk happens after the RPC we waited on above will have fired
	// There is no deterministic way to know once its been written so we wrap this in a retry.
	retry.Run(t, func(r *retry.R) {
		resp, err := testAC.ac.readPersistedAutoConfig()
		require.NoError(r, err)

		// ensure the roots got persisted to disk
		require.Equal(r, thirdCert.CertPEM, resp.Certificate.GetCertPEM())
		require.Equal(r, secondRoots.ActiveRootID, resp.CARoots.GetActiveRootID())
	})
}
