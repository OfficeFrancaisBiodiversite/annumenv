from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingMultiStepFeedback
from qgis.core import QgsProcessingParameterRasterDestination
import processing


class Modle(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterRasterDestination('Input_4bande_crop', 'Input_4bande_crop', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterRasterDestination('Input_4bandestif', 'Input_4bandes.tif', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterRasterDestination('Smoo_input_4b_crop', 'Smoo_input_4b_crop', createByDefault=True, defaultValue=None))

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(3, model_feedback)
        results = {}
        outputs = {}

        # Fusionner
        alg_params = {
            'DATA_TYPE': 5,
            'EXTRA': '',
            'INPUT': [['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/SENTINEL2B_20211122-111801-162_L2A_T30TWT_D/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0_FRE_B2.tif'],['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/SENTINEL2B_20211122-111801-162_L2A_T30TWT_D/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0_FRE_B3.tif'],['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/SENTINEL2B_20211122-111801-162_L2A_T30TWT_D/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0_FRE_B4.tif'],['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/SENTINEL2B_20211122-111801-162_L2A_T30TWT_D/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0/SENTINEL2B_20211122-111801-162_L2A_T30TWT_C_V3-0_FRE_B8.tif']],
            'NODATA_INPUT': None,
            'NODATA_OUTPUT': None,
            'OPTIONS': '',
            'PCT': False,
            'SEPARATE': True,
            'OUTPUT': parameters['Input_4bandestif']
        }
        outputs['Fusionner'] = processing.run('gdal:merge', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Input_4bandestif'] = outputs['Fusionner']['OUTPUT']

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # Découper un raster selon une couche de masque
        alg_params = {
            'ALPHA_BAND': False,
            'CROP_TO_CUTLINE': True,
            'DATA_TYPE': 0,
            'EXTRA': '',
            'INPUT': outputs['Fusionner']['OUTPUT'],
            'KEEP_RESOLUTION': False,
            'MASK': 'C:/Users/hugop/OneDrive/Documents/MyGis/GIS_datas/BDTOPO_3-0_INFOS_SHP_WGS84G_FRA_2021-09-15/MD_DEPARTEMENT.shp',
            'MULTITHREADING': False,
            'NODATA': None,
            'OPTIONS': '',
            'SET_RESOLUTION': False,
            'SOURCE_CRS': None,
            'TARGET_CRS': None,
            'X_RESOLUTION': None,
            'Y_RESOLUTION': None,
            'OUTPUT': parameters['Input_4bande_crop']
        }
        outputs['DcouperUnRasterSelonUneCoucheDeMasque'] = processing.run('gdal:cliprasterbymasklayer', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Input_4bande_crop'] = outputs['DcouperUnRasterSelonUneCoucheDeMasque']['OUTPUT']

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # DynamicConvert
        alg_params = {
            'channels': 'all',
            'channels.grayscale.channel': 1,
            'channels.rgb.blue': 0,
            'channels.rgb.green': 0,
            'channels.rgb.red': 0,
            'in': outputs['DcouperUnRasterSelonUneCoucheDeMasque']['OUTPUT'],
            'mask': None,
            'outmax': 255,
            'outmin': 0,
            'outputpixeltype': 5,
            'quantile.high': 2,
            'quantile.low': 2,
            'type': 'linear',
            'type.linear.gamma': 1,
            'out': parameters['Smoo_input_4b_crop']
        }
        outputs['Dynamicconvert'] = processing.run('otb:DynamicConvert', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Smoo_input_4b_crop'] = outputs['Dynamicconvert']['out']
        return results

    def name(self):
        return 'Modèle'

    def displayName(self):
        return 'Modèle'

    def group(self):
        return ''

    def groupId(self):
        return ''

    def createInstance(self):
        return Modle()
