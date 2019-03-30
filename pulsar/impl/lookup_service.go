package impl

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/url"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
)

type LookupResult struct {
	LogicalAddr  *url.URL
	PhysicalAddr *url.URL
}

type LookupService interface {
	Lookup(topic string) (*LookupResult, error)
}

type lookupService struct {
	rpcClient  RpcClient
	serviceUrl *url.URL
}

func NewLookupService(rpcClient RpcClient, serviceUrl *url.URL) LookupService {
	return &lookupService{
		rpcClient:  rpcClient,
		serviceUrl: serviceUrl,
	}
}

func (ls *lookupService) Lookup(topic string) (*LookupResult, error) {
	// Follow brokers redirect up to certain number of times
	const lookupResultMaxRedirect = 20

	for i := 0; i < lookupResultMaxRedirect; i++ {
		id := ls.rpcClient.NewRequestId()
		res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
			RequestId: &id,
			Topic:     &topic,
		})

		if err != nil {
			return nil, err
		}

		log.Infof("Got lookup response: %s", res)
		lr := res.Response.LookupTopicResponse
		switch *lr.Response {
		case pb.CommandLookupTopicResponse_Redirect:
			// TODO: Handle redirects
			log.WithField("topic", topic).Infof("Follow redirect to broker. %v / %v - Use proxy: %v",
				lr.BrokerServiceUrl, lr.BrokerServiceUrlTls, lr.ProxyThroughServiceUrl)
			break

		case pb.CommandLookupTopicResponse_Connect:
			log.WithField("topic", topic).Infof("Successfully looked up topic on broker. %s / %s - Use proxy: %t",
				lr.GetBrokerServiceUrl(), lr.GetBrokerServiceUrlTls(), lr.GetProxyThroughServiceUrl())

			logicalAddress, err := url.Parse(lr.GetBrokerServiceUrl())
			if err != nil {
				return nil, err
			}

			var physicalAddr *url.URL
			if lr.GetProxyThroughServiceUrl() {
				physicalAddr = ls.serviceUrl
			} else {
				physicalAddr = logicalAddress
			}
			return &LookupResult{
				LogicalAddr:  logicalAddress,
				PhysicalAddr: physicalAddr,
			}, nil

		case pb.CommandLookupTopicResponse_Failed:
			log.WithField("topic", topic).Warn("Failed to lookup topic",
				lr.Error.String())
			return nil, errors.New(fmt.Sprintf("failed to lookup topic: %s", lr.Error.String()))
		}
	}

	return nil, nil
}
