/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cassandra;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class ResampleDate
{
    private ResampleDate()
    {
    }

    @ScalarFunction("resample_date")
    @Description("Returns the end date of the selected sample interval size")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long resampleDate(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long ts, @SqlType(StandardTypes.VARCHAR) Slice intervalSlice)
    {
        final String interval = intervalSlice.toStringUtf8();
        final int invervalLength = Integer.parseInt(interval.substring(0, interval.length() - 1));
        final DateTime hour = new DateTime(ts).hourOfDay().roundFloorCopy();

        if (interval.toLowerCase().endsWith("m")) {
            final long minutes = new Duration(hour, new DateTime(ts)).getStandardMinutes();
            //Get minutes since hour and divide by interval, so like this:
            //23 minutes / 5 minute interval = 4.  We multiply that by 4 to get 20 and add the interval length
            //which gets the end point of the interval 23 -> 25.
            //Add that back to the truncated hour value and we have the full ts resampled
            final long roundedMinutes = ((minutes / invervalLength) * invervalLength) + invervalLength;
            return hour.plusMinutes(Long.valueOf(roundedMinutes).intValue()).getMillis();
        }
        else if (interval.toLowerCase().endsWith("h")) {
            return hour.plusHours(1).getMillis();
        }
        else {
            return 0;
        }
    }
}
