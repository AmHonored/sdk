package futureq

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/futureq-io/protocol/proto/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type subscriptionTestServer struct {
	pb.UnimplementedFutureQConsumerServer
	mu            sync.Mutex
	subscriptions map[string]*pb.SubscriptionResponse
	sequence      int
	binds         chan *pb.SubscribeInit
}

func subscriptionTestKey(topic, group, subscriber string) string {
	return fmt.Sprintf("%q/%q/%q", topic, group, subscriber)
}
func (s *subscriptionTestServer) CreateSubscription(_ context.Context, r *pb.SubscriptionRequest) (*pb.SubscriptionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := subscriptionTestKey(r.Topic, r.GroupId, r.SubscriberId)
	if existing := s.subscriptions[key]; existing != nil {
		return existing, nil
	}
	s.sequence++
	result := &pb.SubscriptionResponse{Topic: r.Topic, GroupId: r.GroupId, SubscriberId: r.SubscriberId, Generation: fmt.Sprint(s.sequence)}
	s.subscriptions[key] = result
	return result, nil
}
func (s *subscriptionTestServer) Unsubscribe(_ context.Context, r *pb.SubscriptionRequest) (*pb.UnsubscribeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, subscriptionTestKey(r.Topic, r.GroupId, r.SubscriberId))
	return &pb.UnsubscribeResponse{}, nil
}
func (s *subscriptionTestServer) Subscribe(stream grpc.BidiStreamingServer[pb.ConsumerFrame, pb.QueueMessage]) error {
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	init := frame.GetInit()
	s.mu.Lock()
	record := s.subscriptions[subscriptionTestKey(init.Topic, init.GroupId, init.SubscriberId)]
	s.mu.Unlock()
	if record == nil {
		return status.Error(codes.NotFound, "subscription missing")
	}
	s.binds <- init
	if err := stream.SendHeader(metadata.Pairs("subscription-generation", record.Generation)); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func TestSubscriptionAPIsAndStableUniversalBinding(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	defer listener.Close()
	mock := &subscriptionTestServer{subscriptions: make(map[string]*pb.SubscriptionResponse), binds: make(chan *pb.SubscribeInit, 4)}
	server := grpc.NewServer()
	pb.RegisterFutureQConsumerServer(server, mock)
	go server.Serve(listener)
	defer server.Stop()
	tracker := newTopologyTracker(nil, time.Hour, func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///subscriptions", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	})
	tracker.setLeader(1, "subscriptions", Topology{})
	client := &Client{tracker: tracker}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	audit := Subscription{Topic: "orders", SubscriberID: "audit"}
	first, err := client.Subscribe(ctx, audit)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := client.Subscribe(ctx, audit)
	if err != nil || first.Generation != repeated.Generation {
		t.Fatalf("idempotent subscribe: %v", err)
	}
	for i := 0; i < 2; i++ {
		consumer, err := client.NewConsumer(ctx, "orders", "", WithSubscriberID("audit"))
		if err != nil {
			t.Fatal(err)
		}
		bind := <-mock.binds
		if bind.SubscriberId != "audit" || bind.GroupId != "" {
			t.Fatalf("wrong binding: %v", bind)
		}
		consumer.Close()
	}
	if _, err := client.NewConsumer(ctx, "orders", ""); err == nil {
		t.Fatal("anonymous universal identity accepted")
	}
	if err := client.Unsubscribe(ctx, audit); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewConsumer(ctx, "orders", "", WithSubscriberID("audit")); status.Code(err) != codes.NotFound {
		t.Fatalf("expected missing subscription, got %v", err)
	}
	newer, err := client.Subscribe(ctx, audit)
	if err != nil || newer.Generation == first.Generation {
		t.Fatalf("recreate: %v", err)
	}
	for _, invalid := range []Subscription{{Topic: "orders"}, {Group: "workers"}, {Topic: "orders", Group: "workers", SubscriberID: "audit"}} {
		if _, err := client.Subscribe(ctx, invalid); err == nil {
			t.Fatalf("invalid identity accepted: %+v", invalid)
		}
	}
}
