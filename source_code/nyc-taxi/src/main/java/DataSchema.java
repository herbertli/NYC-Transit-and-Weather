import java.util.ArrayList;

class DataSchema {

    static ArrayList<String> extractYellow(String[] rowSplit) {
        /*
        VendorID
        tpep_pickup_datetime
        tpep_dropoff_datetime
        passenger_count
        trip_distance
        RatecodeID
        store_and_fwd_flag
        PULocationID
        DOLocationID
        payment_type
        fare_amount
        extra
        mta_tax
        tip_amount
        tolls_amount
        improvement_surcharge
        total_amount
        */
        return extractCol(rowSplit, 1, 2, 3, 4, 7, 8);
    }

    static ArrayList<String> extractGreen(String[] rowSplit) {
        /*
        VendorID
        lpep_pickup_datetime
        lpep_dropoff_datetime
        store_and_fwd_flag
        RatecodeID
        PULocationID
        DOLocationID
        passenger_count
        trip_distance
        fare_amount
        extra
        mta_tax
        tip_amount
        tolls_amount
        ehail_fee
        improvement_surcharge
        total_amount
        payment_type
        trip_type
         */
        return extractCol(rowSplit, 1, 2, 5, 6, 7, 8);

    }

    static ArrayList<String> extractFHV(String[] rowSplit) {
        /*
        Dispatching_base_num
        Pickup_DateTime
        DropOff_datetime
        PUlocationID
        DOlocationID
         */
        return extractCol(rowSplit, 1, 2, 3, 4);
    }

    private static ArrayList<String> extractCol(String[] rowSplit, int...indices) {
        ArrayList<String> selectedCols = new ArrayList<>();
        for (int i: indices) {
            if (i >= rowSplit.length) return null;
            String col = rowSplit[i];
            String cleanedCol = col.replace("\"", "");
            selectedCols.add(cleanedCol);
        }
        return selectedCols;
    }

}
