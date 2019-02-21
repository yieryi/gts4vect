package com.wangkang.gts4vect;


import org.apache.log4j.Logger;
import org.geotools.data.DataStore;

/**
 * @author wangkang
 * @email iwuang@qq.com
 * @date 2019/1/24 14:46
 */
public class App {

    public static void main(String[] args) {

        PGDatastore pgDatastore = new PGDatastore();
        DataStore datastore = PGDatastore.getDefeaultDatastore();
        Geotools geotools = new Geotools(datastore);
        String geojsonpath = "C:\\test\\ChinaWorldCitysBigbelin\\chinaCompany2.geojson";
        String shpfilepath = "C:\\test\\ChinaWorldCitysBigbelin\\MuchBig.shp";
        String pgtableName = "MuchBigPolygon";

//        geotools.geojson2pgtable(geojsonpath, pgtableName);
        geotools.geojson2shp(geojsonpath, shpfilepath);
//        geotools.shp2geojson(shpfilepath, geojsonpath);
//        geotools.shp2pgtable(shpfilepath, pgtableName);
        utility.tagLast("shp导入postgis");

//        geotools.pgtable2geojson(pgtableName, geojsonpath);
//        geotools.pgtable2shp(pgtableName, shpfilepath, "geom");


    }

    private static Logger logger = Logger.getLogger(App.class);
    private static Utility utility = new Utility();

}
