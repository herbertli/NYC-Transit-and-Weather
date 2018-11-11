package herbertli.nyc.taxi;

import java.util.ArrayList;

class DataSchema {


    static ArrayList<String> extractYellow(String[] rowSplit) {
        /*
        Data is in form:
        2,2017-01-31 23:59:58,2017-02-01 00:20:01,1,4.95,1,N,114,112,1,18,0.5,0.5,3.86,0,0.3,23.16
        0 - VendorID
        1 - tpep_pickup_datetime
        2 - tpep_dropoff_datetime
        3 - Passenger_count
        4 - Trip_distance
        5 - PULocationID
        6 - DOLocationID
        7 - RateCodeID
        8 - Store_and_fwd_flag
        9 - Payment_type
        10 - Fare_amount
        11 - Extra
        12 - MTA_tax
        13 - Improvement_surcharge
        14 - Tip_amount
        15 - Tolls_amount
        16 - Total_amount
         */
        return extractCol(rowSplit, 1, 2, 3, 4, 5, 6);
    }

    static ArrayList<String> extractGreen(String[] rowSplit) {
        /*
        Data is in form:
        2,2017-01-31 23:59:58,2017-02-01 00:20:01,1,4.95,1,N,114,112,1,18,0.5,0.5,3.86,0,0.3,23.16
        0 - VendorID
        1 - lpep_pickup_datetime
        2 - lpep_dropoff_datetime
        3 - Passenger_count
        4 - Trip_distance
        5 - PULocationID
        6 - DOLocationID
        7 - RateCodeID
        8 - Store_and_fwd_flag
        9 - Payment_type
        10 - Fare_amount
        11 - Extra
        12 - MTA_tax
        13 - Improvement_surcharge
        14 - Tip_amount
        15 - Tolls_amount
        16 - Total_amount
        17 - Trip_type
         */
        return extractCol(rowSplit, 1, 2, 3, 4, 5, 6);

    }

    static ArrayList<String> extractFHV(String[] rowSplit) {
        /*
        Data is in form:
        2,2017-01-31 23:59:58,2017-02-01 00:20:01,1,4.95,1,N,114,112,1,18,0.5,0.5,3.86,0,0.3,23.16
        0 - Dispatching_base_num
        1 - Pickup_datetime
        2 - DropOff_datetime
        3 - PULocationID
        4 - DOLocationID
        5 - SR_Flag
         */
        return extractCol(rowSplit, 0, 1, 2, 3, 4, 5);
    }

    private static ArrayList<String> extractCol(String[] rowSplit, int...indices) {
        ArrayList<String> selectedCols = new ArrayList<>();
        for (int i: indices) {
            selectedCols.add(rowSplit[i]);
        }
        return selectedCols;
    }

}
