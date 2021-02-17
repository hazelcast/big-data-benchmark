/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.hazelcast.jet.benchmark.nexmark.ClosedAuctionP.closedAuction;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class Q06AvgSellingPrice extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int auctionIdModulus = 128;
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = Math.max(1, eventsPerSecond / (8192 * auctionIdModulus));
        int auctionBidRatio = 10;
        long auctionMaxDuration = 1024;
        int windowItemCount = 10;

        // We generate auctions at rate eventsPerSecond / auctionBidRatio.
        // We generate bids at rate eventsPerSecond, each bid refers to auctionId = seq / auctionBidRatio

        StreamStage<Object> auctions = pipeline
                .readFrom(EventSourceP.eventSource(eventsPerSecond / auctionBidRatio, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = getRandom(271 * seq, auctionMaxDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0)
                .map(p -> p);

        StreamStage<Object> bids = pipeline
                .readFrom(EventSourceP.eventSource(eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> new Bid(seq, timestamp, seq / auctionBidRatio, 0)))
                .withNativeTimestamps(0)
                .map(p -> p);

        return auctions
                .merge(bids)
                .apply(closedAuction(auctionMaxDuration))
                .groupingKey(t -> t.f0().sellerId())
                .mapStateful(() -> new LongRingBuffer(windowItemCount),
                        (ctx, key, item) -> {
                            ctx.add(item.f1().price());
                            return tuple2(ctx.avg(), item.f0().expires());
                        })

                .filter(t -> t.f1() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, Tuple2::f1));
    }

    private static final class LongRingBuffer {
        private final long[] data;
        private int ptr;
        private int size;

        LongRingBuffer(int capacity) {
            data = new long[capacity];
        }

        public void add(long value) {
            data[ptr++] = value;
            if (ptr == data.length) {
                ptr = 0;
            }
            if (size < data.length) {
                size++;
            }
        }

        public long avg() {
            long total = 0;
            for (int i = 0, pos = ptr; i < size; i++) {
                if (pos == 0) {
                    pos = data.length - 1;
                } else {
                    pos--;
                }
                total += data[pos];
            }
            return total / size;
        }
    }
}
