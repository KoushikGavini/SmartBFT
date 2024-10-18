package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestTableDriven(t *testing.T) {
	tests := []struct {
		name           string
		numberOfNodes  int
		setupFunc      func([]*App)
		testFunc       func(*testing.T, []*App, *Network)
		cleanupFunc    func([]*App)
		expectedResult func(*testing.T, []*App)
	}{
		{
			name:          "Basic consensus",
			numberOfNodes: 4,
			setupFunc: func(nodes []*App) {
				startNodes(nodes, NewNetwork())
			},
			testFunc: func(t *testing.T, nodes []*App, network *Network) {
				for i := 1; i < 5; i++ {
					nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
				}
			},
			expectedResult: func(t *testing.T, nodes []*App) {
				data := make([]*AppRecord, 0)
				for i := 0; i < len(nodes); i++ {
					d := <-nodes[i].Delivered
					data = append(data, d)
				}
				for i := 0; i < len(nodes)-1; i++ {
					assert.Equal(t, data[i], data[i+1])
				}
			},
		},
		{
			name:          "Leader in partition",
			numberOfNodes: 4,
			setupFunc: func(nodes []*App) {
				network := NewNetwork()
				startNodes(nodes, network)
				nodes[0].Disconnect() // leader in partition
			},
			testFunc: func(t *testing.T, nodes []*App, network *Network) {
				for i := 1; i < len(nodes); i++ {
					nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
				}
			},
			expectedResult: func(t *testing.T, nodes []*App) {
				data := make([]*AppRecord, 0)
				for i := 1; i < len(nodes); i++ {
					d := <-nodes[i].Delivered
					data = append(data, d)
				}
				for i := 0; i < len(nodes)-2; i++ {
					assert.Equal(t, data[i], data[i+1])
				}
				assert.LessOrEqual(t, uint64(2), nodes[2].Consensus.GetLeaderID())
			},
		},
		{
			name:          "Catching up with view change",
			numberOfNodes: 4,
			setupFunc: func(nodes []*App) {
				network := NewNetwork()
				start := time.Now()
				for _, n := range nodes {
					n.viewChangeTime = make(chan time.Time, 1)
					n.viewChangeTime <- start
				}
				startNodes(nodes, network)
				nodes[3].Disconnect() // will need to catch up
			},
			testFunc: func(t *testing.T, nodes []*App, network *Network) {
				nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

				data := make([]*AppRecord, 0)
				for i := 0; i < len(nodes)-1; i++ {
					d := <-nodes[i].Delivered
					data = append(data, d)
				}

				nodes[3].Connect()
				nodes[0].Disconnect() // leader in partition

				for i := 1; i < len(nodes); i++ {
					nodes[i].Submit(Request{ID: "2", ClientID: "alice"}) // submit to other nodes
				}

				done := make(chan struct{})
				defer close(done)

				var counter uint64
				accelerateTime(nodes, done, false, true, &counter)
			},
			expectedResult: func(t *testing.T, nodes []*App) {
				data3 := <-nodes[3].Delivered // from catch up
				assert.NotNil(t, data3)

				data := make([]*AppRecord, 0)
				for i := 1; i < len(nodes); i++ {
					d := <-nodes[i].Delivered
					data = append(data, d)
				}
				for i := 0; i < len(nodes)-2; i++ {
					assert.Equal(t, data[i], data[i+1])
				}
			},
		},
		{
			name:          "Restart followers",
			numberOfNodes: 4,
			setupFunc: func(nodes []*App) {
				network := NewNetwork()
				startNodes(nodes, network)
			},
			testFunc: func(t *testing.T, nodes []*App, network *Network) {
				nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

				data := make([]*AppRecord, 0)
				d0 := <-nodes[0].Delivered
				d2 := <-nodes[2].Delivered
				d3 := <-nodes[3].Delivered

				nodes[1].Restart()
				d1 := <-nodes[1].Delivered

				data = append(data, d0, d1, d2, d3)

				nodes[2].Restart()

				nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

				d0 = <-nodes[0].Delivered
				d1 = <-nodes[1].Delivered
				d2 = <-nodes[2].Delivered

				nodes[3].Restart()
				d3 = <-nodes[3].Delivered

				data = append(data, d0, d1, d2, d3)
			},
			expectedResult: func(t *testing.T, nodes []*App) {
				for i := 0; i < len(nodes)-1; i++ {
					assert.Equal(t, nodes[i].lastDecision, nodes[i+1].lastDecision)
				}
			},
		},
		{
			name:          "Blacklist and redemption",
			numberOfNodes: 10,
			setupFunc: func(nodes []*App) {
				network := NewNetwork()
				start := time.Now()
				for _, n := range nodes {
					n.heartbeatTime = make(chan time.Time, 1)
					n.heartbeatTime <- start
					n.viewChangeTime = make(chan time.Time, 1)
					n.viewChangeTime <- start
					n.Setup()
				}
				startNodes(nodes, network)
			},
			testFunc: func(t *testing.T, nodes []*App, network *Network) {
				nodes[0].Disconnect() // Leader is in partition

				// Accelerate the time until a view change
				done := make(chan struct{})
				var counter uint64
				accelerateTime(nodes[1:], done, true, true, &counter)

				// Rotate the leader and ensure the view doesn't change
				for j := 0; j < len(nodes); j++ {
					nodes[1].Submit(Request{ID: fmt.Sprintf("%d", j), ClientID: "alice"})
					for i := 1; i < len(nodes); i++ {
						<-nodes[i].Delivered
					}
				}

				// Re-connect node 1
				nodes[0].Connect()

				// Accelerate time to wait for it to synchronize
				done = make(chan struct{})
				accelerateTime(nodes, done, true, true, &counter)
				for j := 0; j < len(nodes); j++ {
					<-nodes[0].Delivered
				}
				close(done)
			},
			expectedResult: func(t *testing.T, nodes []*App) {
				md := &smartbftprotos.ViewMetadata{}
				err := proto.Unmarshal(nodes[1].lastDecision.Proposal.Metadata, md)
				assert.NoError(t, err)
				assert.Equal(t, uint64(1), md.ViewId)
				assert.Empty(t, md.BlackList)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network := NewNetwork()
			defer network.Shutdown()

			testDir, err := os.MkdirTemp("", t.Name())
			assert.NoErrorf(t, err, "generate temporary test dir")
			defer os.RemoveAll(testDir)

			nodes := make([]*App, 0)
			for i := 1; i <= tt.numberOfNodes; i++ {
				n := newNode(uint64(i), network, t.Name(), testDir, true, 1)
				nodes = append(nodes, n)
			}

			tt.setupFunc(nodes)
			tt.testFunc(t, nodes, network)
			if tt.cleanupFunc != nil {
				tt.cleanupFunc(nodes)
			}
			tt.expectedResult(t, nodes)
		})
	}
}

func newNode(id uint64, network *Network, testName, testDir string, leaderRotation bool, decisionsPerLeader uint64) *App {
	n := &App{
		ID:                   id,
		Network:              network,
		testName:             testName,
		testDir:              testDir,
		leaderRotation:       leaderRotation,
		decisionsPerLeader:   decisionsPerLeader,
		Delivered:            make(chan *AppRecord, 100),
		logger:               zap.NewExample().Sugar(),
		verificationSequence: 1,
	}
	n.Setup()
	return n
}

func startNodes(nodes []*App, network *Network) {
	for _, n := range nodes {
		if err := n.Consensus.Start(); err != nil {
			n.logger.Panicf("Consensus returned an error : %v", err)
		}
	}
	network.StartServe()
}

func accelerateTime(nodes []*App, done chan struct{}, accelerateHeartbeat, accelerateViewChange bool, counter *uint64) {
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 10):
				for _, n := range nodes {
					if accelerateHeartbeat {
						select {
						case n.heartbeatTime <- time.Now().Add(time.Second * time.Duration(10+*counter)):
						default:
						}
					}
					if accelerateViewChange {
						select {
						case n.viewChangeTime <- time.Now().Add(time.Minute * time.Duration(10+*counter)):
						default:
						}
					}
				}
				*counter++
			}
		}
	}()
}
