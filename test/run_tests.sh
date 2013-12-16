#!/usr/bin/env bash
echo
echo '---- IF-DREAM-O-UpdateQualityMD --------'
( cd updateQualityMD_sa; ./test_uqmd.py )
echo
echo '---- IF-DREAM-O-AddProduct --------'
( cd addProduct_sa; ./test_addProd.py )
echo
echo '---- IF-DREAM-O-ManageScenario --------'
( cd manageScenario_sa; ./test_manageScenario.py )
