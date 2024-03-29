package com.example.jet.benchmark.nexmark;

import com.example.jet.benchmark.nexmark.model.Auction;
import com.example.jet.benchmark.nexmark.model.Person;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple5;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.example.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.jet.Traversers.empty;
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
        // This code respects numDistinctKeys indirectly, by creating a pattern of
        // seller IDs over time so that there are auctions with a given seller ID
        // for a limited time. A "cloud" of random seller IDs slowly moves up the
        // integer line. It advances by one every auctionsPerSeller. The width
        // of the cloud is numDistinckKeys. We calculate the TTL for the keyed
        // mapStateful stage to match the amount of time during which this cloud
        // covers any given seller ID.
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int auctionsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int auctionsPerSeller = 100;
        long ttl = (long) numDistinctKeys * auctionsPerSeller * 1000 / auctionsPerSecond;

        StreamStage<Object> sellers = pipeline
                .readFrom(eventSource("sellers", auctionsPerSecond / auctionsPerSeller, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long id = seq;
                            return new Person(id, timestamp, "Seller #" + id, STATES[seq.intValue() % STATES.length]);
                        }))
                .withNativeTimestamps(0)
                .filter(p  -> p.state().equals("OR") || p.state().equals("CA") || p.state().equals("ID"))
                .map(p -> p); // upcast

        StreamStage<Auction> auctions = pipeline
                .readFrom(eventSource("auctions", auctionsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) -> {
                    long sellerId = seq / auctionsPerSeller - getRandom(seq, numDistinctKeys);
                    if (sellerId < 0) {
                        return new Auction(seq, timestamp, 0, 1, 0); // will be filtered out
                    }
                    int categoryId = (int) getRandom(seq, 10);
                    return new Auction(seq, timestamp, sellerId, categoryId, timestamp + ttl);
                }))
                .withNativeTimestamps(0)
                .filter(a -> a.category() == 0);

        // NEXMark Query 3 start
        StreamStage<Tuple5<String, String, Long, Long, Integer>> queryResult = sellers
                .merge(auctions)
                .groupingKey(o -> o instanceof Person ? ((Person) o).id() : ((Auction) o).sellerId())
                .flatMapStateful(ttl, JoinAuctionToSeller::new, JoinAuctionToSeller::flatMap,
                        (state, key, wm) -> empty());
        // NEXMark Query 3 end

        // queryResult: Tuple5(sellerName, sellerState, auctionStart, auctionId, auctionCategory)
        return queryResult.apply(determineLatency(Tuple5::f2));
    }

    private static final class JoinAuctionToSeller implements Serializable {
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
                return person != null
                        ? singleton(tuple5(
                                person.name(), person.state(), auction.timestamp(), auction.id(), auction.category()))
                        : empty();
            }
        }
    }
}
