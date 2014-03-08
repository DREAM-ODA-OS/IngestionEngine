############################################################
#  Project: DREAM
#
#  Module:  Task 5 ODA Ingestion Engine 
#
#  Author: Milan Novacek (CVC)
#
#  Date:   Jan 9, 2014
#
#    (c) 2014 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine: Perform Coastline Check
#   checks if the area described in the metadata is within
#   the coastline polygon
#
#  used by ingestion_logic
#
############################################################

import logging
import json
import os.path
import traceback

if __name__ == '__main__':
    # Enable stand-alone testing without django
    IE_DEBUG = 2
    from utils import DummyLogger
    logger = DummyLogger()
else:
    from settings import \
        IE_DEBUG
    logger = logging.getLogger('dream.file_logger')


from utils import \
    IngestionError, \
    Bbox

is_ie_c_check = False
shp_driver = None
mem_driver = None

try:
    import osgeo.gdal as gdal
    import osgeo.ogr as ogr
    shp_driver = ogr.GetDriverByName('ESRI Shapefile')
    mem_driver = ogr.GetDriverByName('Memory')
    is_ie_c_check = True
except Exception as e:
    print "ERROR: cannot import/initialise osgeo/ogr; coastline check will fail"
    is_ie_c_check = False

def coastline_ck(coverageDescription, ccache):
    if IE_DEBUG > 0:
        logger.info('  performing coastline_check')

    logger.error('  NOT IMPLEMENTED: coastline_check is not implemented')

    return True

def coastline_cache_from_aoi(shpfile, prjfile, aoi):
    if not is_ie_c_check:
        logger.error("Coastline check is not enabled:"+
                     " could not import osgeo.")
        raise IngestionError("Coastline check failed prerequisites.")

    if None != prjfile:
        logger.error("Only WGS84 is supported for coastline check geometry")

    if IE_DEBUG > 0:
        logger.debug('Calculating clipped_coastline_from_aoi ' + `aoi`)

    src_data_source = shp_driver.Open(shpfile, 0)
    if None == src_data_source:
        logger.error("OGR cannot read " + shpfile)
        raise IngestionError("Error initialising 30km-coastline.")

    src_layer = src_data_source.GetLayer()
    if None == src_layer:
        logger.error("Error getting layer from " + shpfile)
        raise IngestionError("Error initialising 30km-coastline.")

    # create new layer
    res_data_source = mem_driver.CreateDataSource("tmp_coastline")
    out_layer = res_data_source.CreateLayer(
        "coast_layer", geom_type=ogr.wkbMultiPolygon)
    out_multipoly = ogr.Geometry(ogr.wkbGeometryCollection)

    # create AOI bbox as an ogr polygon
    wkt = "POLYGON ((%f %f, %f %f, %f %f, %f %f , %f %f ))" % ( \
        aoi.ll[0], aoi.ll[1],
        aoi.ll[0], aoi.ur[1],
        aoi.ur[0], aoi.ur[1],
        aoi.ur[0], aoi.ll[1],
        aoi.ll[0], aoi.ll[1] )
    ogr_bbox = ogr.CreateGeometryFromWkt(wkt)
    #src_layer.SetSpatialFilter(ogr_bbox)

    feature = src_layer.GetNextFeature()
    if not feature:
        logger.error("Error getting any features from " + shpfile)
        raise IngestionError("Error initialising 30km-coastline.")

    while feature:
        geom = feature.GetGeometryRef()
        count = geom.GetGeometryCount()
        n_outside = 0
        n_inside = 0
        n_intesects = 0
        n_bb_contains = 0
        for i in range(count):
            keep = False
            poly = geom.GetGeometryRef(i)
            if poly.Contains(ogr_bbox):
                n_inside += 1
                keep = True
            else:
                n_outside  += 1
            if poly.Intersects(ogr_bbox):
                n_intesects += 1
                keep = True
            if ogr_bbox.Contains(poly):
                n_bb_contains += 1
                keep = True

            if keep:
                cloned = poly.Clone()
                err = out_multipoly.AddGeometry(cloned)
                if err != 0:
                    logger.error(
                        "Internal Error construcing geom-cache, e="+ err)
                    raise IngestionError("Error initialising 30km-coastline.")

        feature.Destroy()
        if IE_DEBUG > 1:
            logger.debug(
                "Cashe coastline subset: " +
                "n in/out/intersects/oo: " + `n_inside` + " / " +
                `n_outside` + " / " +`n_intesects`  + " / " +
                `n_bb_contains`)
        feature = src_layer.GetNextFeature()
 
    src_layer = None
    src_data_source.Destroy()

    out_feature_def = out_layer.GetLayerDefn()
    out_feature  = ogr.Feature(out_feature_def)
    out_feature.SetGeometry(out_multipoly)
    out_layer.CreateFeature(out_feature)
    return res_data_source

def destrory_coastline_cache(coastline_data):
    if None == coastline_data:
        return

# ----------------- Stand-alone test/debug --------------------------
if __name__ == '__main__':
    data_dir = os.path.join( os.getcwd(), 'media', 'etc', 'coastline_data' )
    shpfile  = os.path.join( data_dir, 'ne_10m_land.shp' )
    # around Denmark
    dk_ll =  (50.282,  5.60656)
    dk_ur =  (59.71651, 13.7335)
    # test2 - TODO delete this
    dk_ll =  (49.853678, 14.123)
    dk_ur =  (50.71651,  15.369 )
    
    aoi_bb = Bbox(dk_ll, dk_ur)

    ccoastline = coastline_cache_from_aoi(shpfile, None, aoi_bb)
    cclayer = ccoastline.GetLayer()
    # extent is returned as ( MinX, MaxX, MinY, MaxY)
    print "Extent: " + `cclayer.GetExtent()`

    feature = cclayer.GetNextFeature()
    if not feature:
        logger.error("NO FEATURE in cclayer!")

    while feature:
        geom = feature.GetGeometryRef()
        if not geom:
            logger.error("NO GEOM!")
            break
        count = geom.GetGeometryCount()
        print " feature "+`geom.GetGeometryName()`+", geom count="+`count`
        for i in range(count):
            keep = False
            poly = geom.GetGeometryRef(i)
            p_count = poly.GetGeometryCount()
            print "  "+`poly.GetGeometryName()`+" p_count: "+`p_count`
            for j in range(p_count):
                g1 = poly.GetGeometryRef(j)
                n_points = g1.GetPointCount()
                if not i%500: print "      n_points: " + ` n_points`
                # for p in range(n_points):
                #     if not i%500 and n_points<20 :
                #         print "(" + `g1.GetPoint(p)` + "), "
                #chull = poly.ConvexHull()
                #nn = chull.ExportToWkt()
                #print " nn: " + `nn`
                #chull = None

        feature = cclayer.GetNextFeature()

        
    destrory_clipped_coastline(ccoastline)
