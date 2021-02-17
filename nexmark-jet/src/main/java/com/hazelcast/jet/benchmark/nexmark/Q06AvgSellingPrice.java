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

import java.util.ArrayDeque;
import java.util.Properties;

import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.jet.benchmark.nexmark.JoinAuctionToWinningBidP.joinAuctionToWinningBid;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class Q06AvgSellingPrice extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = Math.max(1, eventsPerSecond / 8192);
        int bidsPerAuction = 10;
        long auctionMaxDuration = 1024;
        long auctionMinDuration = auctionMaxDuration / 2;
        long maxBid = 1000;
        int windowItemCount = 10;

        // We generate auctions at rate eventsPerSecond / bidsPerAuction.
        // We generate bids at rate eventsPerSecond, each bid refers to
        // auctionId = seq / bidsPerAuction

        StreamStage<Object> auctions = pipeline
                .readFrom(eventSource(eventsPerSecond / bidsPerAuction, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = auctionMinDuration +
                                    getRandom(271 * seq, auctionMaxDuration - auctionMinDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0)
                .map(p -> p);

        StreamStage<Object> bids = pipeline
                .readFrom(eventSource(eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long price = getRandom(seq, maxBid);
                            long auctionId = seq / bidsPerAuction;
                            return new Bid(seq, timestamp, auctionId, price);
                        }))
                .withNativeTimestamps(0)
                .map(p -> p);

        return auctions
                .merge(bids)
                .apply(joinAuctionToWinningBid(auctionMaxDuration))
                .groupingKey(t -> t.f0().sellerId())
                .mapStateful(() -> new ArrayDeque<Long>(windowItemCount),
                        (deque, key, item) -> {
                            if (deque.size() == windowItemCount) {
                                deque.removeFirst();
                            }
                            deque.addLast(item.f1().price());
                            return tuple2(deque.stream().mapToLong(i -> i).average(), item.f0().expires());
                        })

                .filter(t -> t.f1() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, Tuple2::f1));
    }
}
