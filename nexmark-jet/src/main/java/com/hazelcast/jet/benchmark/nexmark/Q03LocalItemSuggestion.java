package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Person;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple5;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.datamodel.Tuple5.tuple5;

public class Q03LocalItemSuggestion extends BenchmarkBase {

    private static final String[] STATES = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA",
            "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
            "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    };

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int auctionIdModulus = 128;
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = Math.max(1, eventsPerSecond / (8192 * auctionIdModulus));

        StreamStage<Object> persons = pipeline
                .readFrom(EventSourceP.eventSource(eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) -> {
                    long id = getRandom(seq, numDistinctKeys);
                    return new Person(id, timestamp, "Seller #" + id, STATES[seq.intValue() % STATES.length]);
                }))
                .withNativeTimestamps(0)
                .filter(p  -> p.state().equals("OR") || p.state().equals("CA") || p.state().equals("ID"))
                .map(p -> p);

        StreamStage<Object> auctions = pipeline
                .readFrom(EventSourceP.eventSource(eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) -> {
                    long sellerId = getRandom(137 * seq, numDistinctKeys);
                    return new Auction(seq, timestamp, sellerId, 0, 0);
                }))
                .withNativeTimestamps(0)
                .filter(a -> a.category() == 10)
                .map(p -> p);

        return persons
                .merge(auctions)
                .groupingKey(o -> o instanceof Person ? ((Person) o).id() : ((Auction) o).sellerId())
                .flatMapStateful(SellerAuctionJoin::new, SellerAuctionJoin::flatMap)

                .filter(t4 -> t4.f3() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, Tuple5::f2));
    }

    private static final class SellerAuctionJoin implements Serializable {
        Person person;
        final List<Auction> auctions = new ArrayList<>();

        public Traverser<Tuple5<String, String, Long, Long, Integer>> flatMap(Long key, Object o) {
            if (o instanceof Person) {
                Person person = (Person) o;
                this.person = person;
                return traverseIterable(auctions)
                        .map(auction -> tuple5(person.name(), person.state(), auction.timestamp(), auction.id(),
                                auction.category()));
            } else {
                Auction auction = (Auction) o;
                auctions.add(auction);
                return singleton(tuple5(person.name(), person.state(), auction.timestamp(), auction.id(),
                        auction.category()));
            }
        }
    }
}
