// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkametricsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkametricsreceiver"

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkametricsreceiver/internal/metadata"
)

type saramaMetrics map[string]map[string]interface{}

type brokerScraper struct {
	client       sarama.Client
	settings     component.ReceiverCreateSettings
	config       Config
	saramaConfig *sarama.Config
	mb           *metadata.MetricsBuilder
}

func (s *brokerScraper) Name() string {
	return brokersScraperName
}

func (s *brokerScraper) start(_ context.Context, _ component.Host) error {
	s.mb = metadata.NewMetricsBuilder(s.config.Metrics, s.settings.BuildInfo)
	return nil
}

func (s *brokerScraper) shutdown(context.Context) error {
	if s.client != nil && !s.client.Closed() {
		return s.client.Close()
	}
	return nil
}

func (s *brokerScraper) scrapeConsumerFetch(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("consumer-fetch-rate-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean.rate"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersConsumerFetchRateAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeIncomingByteRate(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("incoming-byte-rate-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean.rate"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersIncomingByteRateAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeOutgoingByteRate(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("outgoing-byte-rate-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean.rate"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersOutgoingByteRateAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeRequestRate(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("request-rate-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean.rate"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersRequestRateAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeResponseRate(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("response-rate-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean.rate"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersResponseRateAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeResponseSize(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("response-size-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersResponseSizeAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeRequestSize(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("request-size-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersRequestSizeAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeRequestsInFlight(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("requests-in-flight-for-broker-", broker.ID())

	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["count"]
			switch v := ms.(type) {
			case int64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersRequestsInFlightDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrapeRequestLatency(allMetrics saramaMetrics, broker *sarama.Broker) {
	key := fmt.Sprint("request-latency-in-ms-for-broker-", broker.ID())
	if metric, ok := allMetrics[key]; ok {
		if metric != nil {
			ms := metric["mean"]
			switch v := ms.(type) {
			case float64:
				brokerID := strconv.Itoa(int(broker.ID()))
				s.mb.RecordKafkaBrokersRequestLatencyAvgDataPoint(pcommon.NewTimestampFromTime(time.Now()), v, brokerID)
			default:
				return
			}
		}
	}
}

func (s *brokerScraper) scrape(context.Context) (pmetric.Metrics, error) {
	if s.client == nil {
		client, err := newSaramaClient(s.config.Brokers, s.saramaConfig)
		if err != nil {
			return pmetric.Metrics{}, fmt.Errorf("failed to create client in brokers scraper: %w", err)
		}
		s.client = client
	}

	brokers := s.client.Brokers()

	allMetrics := make(map[string]map[string]interface{})

	if s.saramaConfig != nil {
		allMetrics = s.saramaConfig.MetricRegistry.GetAll()
	}

	for _, broker := range brokers {
		broker := broker
		s.scrapeConsumerFetch(allMetrics, broker)
		s.scrapeIncomingByteRate(allMetrics, broker)
		s.scrapeOutgoingByteRate(allMetrics, broker)
		s.scrapeRequestLatency(allMetrics, broker)
		s.scrapeRequestRate(allMetrics, broker)
		s.scrapeRequestSize(allMetrics, broker)
		s.scrapeRequestsInFlight(allMetrics, broker)
		s.scrapeResponseRate(allMetrics, broker)
		s.scrapeResponseSize(allMetrics, broker)
	}

	s.mb.RecordKafkaBrokersDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(len(brokers)))

	return s.mb.Emit(), nil
}

func createBrokerScraper(_ context.Context, cfg Config, saramaConfig *sarama.Config,
	settings component.ReceiverCreateSettings) (scraperhelper.Scraper, error) {
	s := brokerScraper{
		settings:     settings,
		config:       cfg,
		saramaConfig: saramaConfig,
	}
	return scraperhelper.NewScraper(
		s.Name(),
		s.scrape,
		scraperhelper.WithStart(s.start),
		scraperhelper.WithShutdown(s.shutdown),
	)
}
