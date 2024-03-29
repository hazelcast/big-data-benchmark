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

package com.example.jet.benchmark.nexmark;

import com.example.jet.benchmark.nexmark.model.Auction;
import com.example.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.ArrayDeque;
import java.util.OptionalDouble;
import java.util.Properties;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.lang.Math.max;

public class Q06AvgSellingPrice extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int bidsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int auctionsPerSecond = 1000;
        int bidsPerAuction = max(1, bidsPerSecond / auctionsPerSecond);
        long auctionMinDuration = (long) numDistinctKeys * bidsPerAuction * 1000 / bidsPerSecond;
        long auctionMaxDuration = 2 * auctionMinDuration;
        System.out.format("Auction duration: %,d .. %,d ms%n", auctionMinDuration, auctionMaxDuration);
        long maxBid = 1000;
        int windowItemCount = 10;

        // We generate auctions at rate eventsPerSecond / bidsPerAuction.
        // We generate bids at rate eventsPerSecond, each bid refers to
        // auctionId = seq / bidsPerAuction

        StreamStage<Object> auctions = pipeline
                .<Object>readFrom(EventSourceP.eventSource("auctions", bidsPerSecond / bidsPerAuction, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = auctionMinDuration +
                                    getRandom(271 * seq, auctionMaxDuration - auctionMinDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0);

        StreamStage<Bid> bids = pipeline
                .readFrom(EventSourceP.eventSource("bids", bidsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long price = getRandom(seq, maxBid);
                            long auctionId = seq / bidsPerAuction;
                            return new Bid(seq, timestamp, auctionId, price);
                        }))
                .withNativeTimestamps(0);

        // NEXMark Query 6 start
        StreamStage<Tuple2<OptionalDouble, Long>> queryResult = auctions
                .merge(bids)
                .apply(JoinAuctionToWinningBidP.joinAuctionToWinningBid(auctionMaxDuration))
                .groupingKey(auctionAndBid -> auctionAndBid.f0().sellerId())
                .mapStateful(() -> new ArrayDeque<Long>(windowItemCount),
                        (deque, key, item) -> {
                            if (deque.size() == windowItemCount) {
                                deque.removeFirst();
                            }
                            deque.addLast(item.f1().price());
                            return tuple2(deque.stream().mapToLong(i -> i).average(), item.f0().expires());
                        });
        // NEXMark Query 6 end

        // queryResult: Tuple2(averagePrice, auctionExpirationTime)
        return queryResult.apply(determineLatency(Tuple2::f1));
    }
}
