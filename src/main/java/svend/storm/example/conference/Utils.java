package svend.storm.example.conference;

import com.google.common.collect.AbstractIterator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import storm.trident.state.OpaqueValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import svend.storm.example.conference.timeline.HourlyTimeline;

/**
 * Created by svend on 01/02/14.
 */
public class Utils {


    private static ObjectMapper mapper = new ObjectMapper();

    public static List<OpaqueValue> opaqueValuesToOpaqueJson(List<OpaqueValue> vals) throws IOException {
        List<OpaqueValue> opaqueJsons = new ArrayList<>(vals.size());
        for (OpaqueValue opaqueVal : vals) {
            String currJson = opaqueVal.getCurr() == null ? null : mapper.writeValueAsString(opaqueVal.getCurr());
            String prevJson = opaqueVal.getPrev() == null ? null : mapper.writeValueAsString(opaqueVal.getPrev());
            opaqueJsons.add(new OpaqueValue(opaqueVal.getCurrTxid(), currJson, prevJson));
        }
        return opaqueJsons;
    }

    public static List<OpaqueValue> opaqueStringToOpaqueValues(List<OpaqueValue> opaqueStrings, Class<HourlyTimeline> expectedClass) throws IOException {
        List<OpaqueValue> opaqueValues = new ArrayList<>(opaqueStrings.size());
        for (OpaqueValue opaqueString : opaqueStrings) {
            Object currVal = opaqueString.getCurr() == null ? null : mapper.readValue((String) opaqueString.getCurr(), expectedClass);
            Object prevVal = opaqueString.getPrev() == null ? null : mapper.readValue((String) opaqueString.getPrev(), expectedClass);
            opaqueValues.add(new OpaqueValue(opaqueString.getCurrTxid(), currVal, prevVal));
        }
        return opaqueValues;
    }


    /**
     * @return a zip iterator loop simultaneously over both list :D
     *
     * written by  Abhinav Sarkar:
     *   https://gist.github.com/abhin4v/5516397
     */
    public static <L,R> Iterable<Pair<L,R>> zip(final Iterable<L> leftCol, final Iterable<R> rightCol) {
        return new Iterable<Pair<L,R>>() {
            @Override
            public Iterator<Pair<L, R>> iterator() {
                final Iterator<L> leftItr = leftCol.iterator();
                final Iterator<R> rightItr = rightCol.iterator();
                return new AbstractIterator<Pair<L,R>>() {
                    @Override
                    protected Pair<L, R> computeNext() {
                        if (leftItr.hasNext() && rightItr.hasNext()) {
                            return Pair.of(leftItr.next(), rightItr.next());
                        } else {
                            return endOfData();
                        }
                    }};
            }};
    }


}
