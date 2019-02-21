package com.wangkang.gts4vect;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import static org.geotools.data.Transaction.AUTO_COMMIT;

/**
 * @author wangkang
 * @email iwuang@qq.com
 * @date 2019/1/24 16:27
 */
public class Geotools {

    private static Logger logger = Logger.getLogger(Geotools.class);
    private static DataStore postgisDatasore;

    /**
     * @param postgisDatasore 静态PGDatastore获取默认，实例化PGDatastore指定自定义
     */
    public Geotools(DataStore postgisDatasore) {
        if (postgisDatasore == null) {
            postgisDatasore = PGDatastore.getDefeaultDatastore();

        }
        this.postgisDatasore = postgisDatasore;
    }

    public Geotools() {
        postgisDatasore = PGDatastore.getDefeaultDatastore();
    }

    /**
     * 通用，要素集写入postgis
     *
     * @param featureCollection
     * @param pgtableName       postgis创建的数据表
     * @return
     */
    public static boolean write2pg(FeatureCollection featureCollection, String pgtableName) {
        boolean result = false;
        try {
            if (Utility.isEmpty(featureCollection) || Utility.isEmpty(pgtableName)) {
                logger.error("参数无效");
                return result;
            }
            SimpleFeatureType simpleFeatureType = (SimpleFeatureType) featureCollection.getSchema();
            SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
            typeBuilder.init(simpleFeatureType);

            typeBuilder.setName(pgtableName);

            SimpleFeatureType newtype = typeBuilder.buildFeatureType();
            postgisDatasore.createSchema(newtype);

            FeatureIterator iterator = featureCollection.features();
            FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter = postgisDatasore.getFeatureWriterAppend(pgtableName, AUTO_COMMIT);

            while (iterator.hasNext()) {
                Feature feature = iterator.next();
                SimpleFeature simpleFeature = featureWriter.next();
                Collection<Property> properties = feature.getProperties();
                Iterator<Property> propertyIterator = properties.iterator();
                while (propertyIterator.hasNext()) {
                    Property property = propertyIterator.next();
                    simpleFeature.setAttribute(property.getName().toString(), property.getValue());
                }
                featureWriter.write();
            }
            iterator.close();
            featureWriter.close();

        } catch (Exception e) {
            logger.error("失败", e);
        }
        return false;
    }

    /**
     * featureCollection写入到shp的datastore
     *
     * @param featureCollection
     * @param shpDataStore
     * @param geomFieldName     featureCollectio中的矢量字段，postgis可以修改使用不同的表名，默认为geom
     * @return
     */
    public static boolean write2shp(FeatureCollection featureCollection, ShapefileDataStore shpDataStore, String geomFieldName) {
        boolean result = false;
        if (Utility.isEmpty(geomFieldName)) {
            geomFieldName = featureCollection.getSchema().getGeometryDescriptor().getType().getName().toString();
        }
        try {
            FeatureIterator<SimpleFeature> iterator = featureCollection.features();
            //shp文件存储写入
            FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter = shpDataStore.getFeatureWriter(shpDataStore.getTypeNames()[0], AUTO_COMMIT);
            while (iterator.hasNext()) {
                Feature feature = iterator.next();
                SimpleFeature simpleFeature = featureWriter.next();
                Collection<Property> properties = feature.getProperties();
                Iterator<Property> propertyIterator = properties.iterator();

                while (propertyIterator.hasNext()) {
                    Property property = propertyIterator.next();
                    if (property.getName().toString().equalsIgnoreCase(geomFieldName)) {
                        simpleFeature.setAttribute("the_geom", property.getValue());
                    } else {
                        simpleFeature.setAttribute(property.getName().toString(), property.getValue());
                    }
                }
                featureWriter.write();
            }
            iterator.close();
            featureWriter.close();
            shpDataStore.dispose();
        } catch (Exception e) {
            logger.error("失败", e);
        }
        return false;
    }

    /**
     * 方法重载，默认使用UTF-8的Shp文件
     * @param geojsonPath
     * @param shpfilepath
     * @return
     */
    public boolean geojson2shp(String geojsonPath, String shpfilepath) {
        return geojson2shp(geojsonPath, shpfilepath, ShpCharset.UTF_8);
    }

    /**
     * Geojson转成shpfile文件
     *
     * @param geojsonPath
     * @param shpfilepath
     * @return
     */
    public boolean geojson2shp(String geojsonPath, String shpfilepath,Charset shpChart) {
        boolean result = false;
        try {
            Utility.valiFileForRead(geojsonPath);
            FeatureJSON featureJSON = new FeatureJSON();
            featureJSON.setEncodeNullValues(true);
            FeatureCollection featureCollection = featureJSON.readFeatureCollection(new InputStreamReader(new FileInputStream(geojsonPath),"utf-8"));

            File file = new File(shpfilepath);
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
            ShapefileDataStore shpDataStore = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

            //postgis获取的Featuretype获取坐标系代码
            SimpleFeatureType pgfeaturetype = (SimpleFeatureType) featureCollection.getSchema();

            SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
            typeBuilder.init(pgfeaturetype);
            typeBuilder.setCRS(DefaultGeographicCRS.WGS84);
            pgfeaturetype = typeBuilder.buildFeatureType();
            //设置成utf-8编码
            shpDataStore.setCharset(shpChart);
            shpDataStore.createSchema(pgfeaturetype);
            write2shp(featureCollection, shpDataStore, "");
            result = true;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * geojson文件写入到postgis里
     *
     * @param geojsonPath
     * @param pgtableName
     * @return
     */
    public boolean geojson2pgtable(String geojsonPath, String pgtableName) {
        boolean result = false;
        try {
            if (Utility.isEmpty(geojsonPath) || Utility.isEmpty(pgtableName)) {
                return result;
            }
            FeatureJSON featureJSON = new FeatureJSON();
            FeatureCollection featureCollection = featureJSON.readFeatureCollection(new FileInputStream(geojsonPath));
            write2pg(featureCollection, pgtableName);
            result = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 重载方法，默认UTF-8编码SHP文件
     *
     * @param shpPath
     * @param geojsonPath
     * @return
     */
    public boolean shp2geojson(String shpPath, String geojsonPath) {
        return shp2geojson(shpPath, geojsonPath, ShpCharset.UTF_8);
    }

    /**
     * shp转成geojson，保留15位小数
     *
     * @param shpPath     shp的路径
     * @param geojsonPath geojson的路径
     * @return
     */
    public boolean shp2geojson(String shpPath, String geojsonPath, Charset shpCharset) {
        boolean result = false;
        try {
            if (!Utility.valiFileForRead(shpPath) || Utility.isEmpty(geojsonPath)) {
                return result;
            }
            ShapefileDataStore shapefileDataStore = new ShapefileDataStore(new File(shpPath).toURI().toURL());
            shapefileDataStore.setCharset(shpCharset);
            ContentFeatureSource featureSource = shapefileDataStore.getFeatureSource();
            ContentFeatureCollection contentFeatureCollection = featureSource.getFeatures();
            FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
            Utility.valiFileForWrite(geojsonPath);
            featureJSON.writeFeatureCollection(contentFeatureCollection, new File(geojsonPath));
            shapefileDataStore.dispose();
            result = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    public boolean shp2pgtable(String shpPath, String pgtableName) {
        return shp2pgtable(shpPath, pgtableName, ShpCharset.UTF_8);
    }

    /**
     * shpfile文件导入到postgis中
     *
     * @param shpPath
     * @param pgtableName
     * @return
     */
    public boolean shp2pgtable(String shpPath, String pgtableName, Charset shpCharset) {
        boolean result = false;
        try {
            ShapefileDataStore shapefileDataStore = new ShapefileDataStore(new File(shpPath).toURI().toURL());
            shapefileDataStore.setCharset(shpCharset);
            FeatureCollection featureCollection = shapefileDataStore.getFeatureSource().getFeatures();
            write2pg(featureCollection, pgtableName);
            result = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * postgis数据表导出到成shpfile
     *
     * @param pgtableName
     * @param shpPath
     * @param geomField   postgis里的字段
     * @return
     */
    public boolean pgtable2shp(String pgtableName, String shpPath, String geomField) {
        boolean result = false;
        try {

            FeatureSource featureSource = postgisDatasore.getFeatureSource(pgtableName);

            // 初始化 ShapefileDataStore
            File file = new File(shpPath);
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
            ShapefileDataStore shpDataStore = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

            //postgis获取的Featuretype获取坐标系代码
            SimpleFeatureType pgfeaturetype = ((SimpleFeatureSource) featureSource).getSchema();
            String srid = pgfeaturetype.getGeometryDescriptor().getUserData().get("nativeSRID").toString();
            SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
            typeBuilder.init(pgfeaturetype);
            if (!srid.equals("0")) {
                CoordinateReferenceSystem crs = CRS.decode("EPSG:" + srid, true);
                typeBuilder.setCRS(crs);
            }
            pgfeaturetype = typeBuilder.buildFeatureType();
            //设置成utf-8编码
            shpDataStore.setCharset(Charset.forName("utf-8"));
            shpDataStore.createSchema(pgfeaturetype);
            write2shp(featureSource.getFeatures(), shpDataStore, geomField);
            result = true;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * postgis指定的数据表转成geojson文件保留15位小数
     *
     * @param pgtableName 表名
     * @param geojsonpath geojson存放位置
     * @return
     */
    public boolean pgtable2geojson(String pgtableName, String geojsonpath) {
        boolean result = false;
        try {
            FeatureSource featureSource = postgisDatasore.getFeatureSource(pgtableName);
            FeatureCollection featureCollection = featureSource.getFeatures();

            FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
            featureJSON.setEncodeNullValues(true);

            String s = featureJSON.toString(featureCollection);
            FileUtils.writeStringToFile(new File(geojsonpath), s, Charsets.toCharset("utf-8"), false);
            result = true;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    public boolean deletePgtable(String pgtableName) {
        boolean result = false;
        try {
            postgisDatasore.removeSchema(pgtableName);
            result = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    /*
    //    测试调试专用，成功清除所有的sw开头的表（用来存储矢量数据的表）
        public boolean clearSWTable() throws Exception {
            postgisDatasore.removeSchema();
            //relkind char r = 普通表，i = 索 引， S = 序列，v = 视 图， m = 物化视图， c = 组合类型，t = TOAST表， f = 外部 表
            String strtables = " select string_agg(relname ,\',\') from pg_class where relname like \'%sw_%\'  and relkind=\'r\' ";
            List list =  postgisDatasore.getSessionFactory().getCurrentSession().createQuery(strtables).list();
            list.get(0).toString();
            Integer integer = 0;
            if (list.size() > 0) {
                integer = temp.getSessionFactory().getCurrentSession().createQuery("drop table " + strtables).executeUpdate();
            }
    //        与表有关联的其他序列自动删除
            String sequence = " select string_agg(relname ,\',\') from pg_class where relname like \'%sw_%\' and relkind=\'S\' and relname!=\'txsw_seq\'";
            resultSet = st.executeQuery(sequence);
            while (resultSet.next()) {
                sequence = resultSet.getString(1);
            }
            System.out.println("所有非txsw_seq的序列：" + sequence);
            i = st.executeUpdate("drop SEQUENCE " + strtables);
            return integer == 0 ? true : false;
        }
    */
    public static boolean testCreatFeature(String featurePath) {
        boolean result = false;
        try {


            result = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

}
