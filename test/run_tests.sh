#!/usr/bin/env bash
echo
echo '---- IF-DREAM-O-UpdateQualityMD --------'
( cd updateQualityMD_sa; ./test_uqmd.py )
echo
echo '---- IF-DREAM-O-AddProduct --------'
( cd addProduct_sa; ./test_addProd.py )
