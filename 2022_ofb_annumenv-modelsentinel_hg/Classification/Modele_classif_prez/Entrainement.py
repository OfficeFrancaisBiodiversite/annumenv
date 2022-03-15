from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingMultiStepFeedback
from qgis.core import QgsProcessingParameterRasterDestination
from qgis.core import QgsProcessingParameterFileDestination
from qgis.core import QgsProcessingParameterVectorDestination
import processing


class AcquisitionStatistiques(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterRasterDestination('Img_smoo_merged', 'Img_smoo_merged', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterRasterDestination('Img_finale', 'Img_finale', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterRasterDestination('Ndvi', 'NDVI', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterRasterDestination('Haralick', 'Haralick', optional=True, createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterFileDestination('Confusion_matriccsv', 'Confusion_matric.csv', optional=True, fileFilter='Tous les fichiers (*.*)', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterFileDestination('Trained_mdlmdl', 'Trained_mdl.mdl', fileFilter='Tous les fichiers (*.*)', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterFileDestination('Rpg_with_prediction', 'RPG_with_prediction', optional=True, fileFilter='Tous les fichiers (*.*)', createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterVectorDestination('Rpg_zstat', 'RPG_ZStat', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, defaultValue=None))

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(7, model_feedback)
        results = {}
        outputs = {}

        # Fusionner
        alg_params = {
            'DATA_TYPE': 5,
            'EXTRA': '',
            'INPUT': [['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/Smoo_S2A_T30UWU_20211122_4bd_crop.tif'],['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/SENTINEL2B_20211122-111804-917_L2A_T30TVT_D/Smoo_S2A_T30UVT_20211122_4bd_crop.tif'],['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/SENTINEL2B_20211122-111750-628_L2A_T30UVU_C_V3-0/Smoo_S2A_T30UVU_20211122_4bd_crop.tif'],['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/SENTINEL2B_20211122-111801-162_L2A_T30TWT_D/Smoo_S2A_T30UWT_20211122_4bd_crop.tif'],['C:/Users/hugop/OneDrive/Documents/OFB/teledec/Datas/S2A_MSIL2A_20211124T110401_N0301_R094_T30UXU_20211124T135907/Smoo_S2A_T30UXU_20211122_4bd_crop.tif']],
            'NODATA_INPUT': None,
            'NODATA_OUTPUT': None,
            'OPTIONS': '',
            'PCT': False,
            'SEPARATE': False,
            'OUTPUT': parameters['Img_smoo_merged']
        }
        outputs['Fusionner'] = processing.run('gdal:merge', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Img_smoo_merged'] = outputs['Fusionner']['OUTPUT']

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # VectorClassifier
        alg_params = {
            'cfield': 'predicted',
            'confmap': False,
            'feat': 'mean0, stdev0, min0, max0,\nmean1, stdev1, min1, max1,\nmean2, stdev2, min2, max2,\nmean3, stdev3, min3, max3,\nmean5, stdev5, min5, max5,\nmean6, stdev6, min6, max6,\nmean7, stdev7, min7, max7,\nmean8, stdev8, min8, max8,\nmean9, stdev9, min9, max9,\nmean10, stdev10, min10, max10,\nmean11, stdev11, min11, max11,\nmean12, stdev12, min12, max12,\nmean13, stdev13, min13, max13,\n',
            'in': 'C:\\Users\\hugop\\OneDrive\\Documents\\OFB\\teledec\\Datas\\RPG\\RPG20_VT_ZStat.shp',
            'instat': '',
            'model': 'C:\\Users\\hugop\\OneDrive\\Bureau\\Mdl_Gers_18_couv.mdl',
            'out': parameters['Rpg_with_prediction']
        }
        outputs['Vectorclassifier'] = processing.run('otb:VectorClassifier', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Rpg_with_prediction'] = outputs['Vectorclassifier']['out']

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # BandMath
        alg_params = {
            'exp': '(((@Input_4 - @Input_3) /(@Input_4 + @Input_3))+1)*100',
            'il': outputs['Fusionner']['OUTPUT'],
            'outputpixeltype': 5,
            'out': parameters['Ndvi']
        }
        outputs['Bandmath'] = processing.run('otb:BandMath', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Ndvi'] = outputs['Bandmath']['out']

        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}

        # HaralickTextureExtraction
        alg_params = {
            'channel': 1,
            'in': outputs['Bandmath']['out'],
            'parameters.max': 255,
            'parameters.min': 0,
            'parameters.nbbin': 8,
            'parameters.xoff': 3,
            'parameters.xrad': 5,
            'parameters.yoff': 3,
            'parameters.yrad': 5,
            'step': 1,
            'texture': 'simple',
            'out': parameters['Haralick']
        }
        outputs['Haralicktextureextraction'] = processing.run('otb:HaralickTextureExtraction', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Haralick'] = outputs['Haralicktextureextraction']['out']

        feedback.setCurrentStep(4)
        if feedback.isCanceled():
            return {}

        # Fusionner
        alg_params = {
            'DATA_TYPE': 5,
            'EXTRA': '',
            'INPUT': [outputs['Fusionner']['OUTPUT'],outputs['Bandmath']['out'],outputs['Haralicktextureextraction']['out']],
            'NODATA_INPUT': None,
            'NODATA_OUTPUT': None,
            'OPTIONS': '',
            'PCT': False,
            'SEPARATE': True,
            'OUTPUT': parameters['Img_finale']
        }
        outputs['Fusionner'] = processing.run('gdal:merge', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Img_finale'] = outputs['Fusionner']['OUTPUT']

        feedback.setCurrentStep(5)
        if feedback.isCanceled():
            return {}

        # ZonalStatistics
        alg_params = {
            'in': outputs['Fusionner']['OUTPUT'],
            'inbv': 0,
            'inzone': 'vector',
            'inzone.labelimage.in': None,
            'inzone.labelimage.nodata': 0,
            'inzone.vector.in': 'C:/Users/hugop/OneDrive/Documents/OFB/RPG2020/RPG20_Veroded.shp',
            'inzone.vector.reproject': False,
            'out': 'vector',
            'out.raster.bv': 0,
            'out.xml.filename': '',
            'outputpixeltype': 5,
            'out.raster.filename': QgsProcessing.TEMPORARY_OUTPUT,
            'out.vector.filename': parameters['Rpg_zstat']
        }
        outputs['Zonalstatistics'] = processing.run('otb:ZonalStatistics', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Rpg_zstat'] = outputs['Zonalstatistics']['out.vector.filename']

        feedback.setCurrentStep(6)
        if feedback.isCanceled():
            return {}

        # TrainVectorClassifier
        alg_params = {
            'cfield': 'Class_Qual',
            'classifier': 'rf',
            'classifier.ann.a': 1,
            'classifier.ann.b': 1,
            'classifier.ann.bpdw': 0.1,
            'classifier.ann.bpms': 0.1,
            'classifier.ann.eps': 0.01,
            'classifier.ann.f': 'sig',
            'classifier.ann.iter': 1000,
            'classifier.ann.rdw': 0.1,
            'classifier.ann.rdwm': 0,
            'classifier.ann.sizes': '',
            'classifier.ann.t': 'reg',
            'classifier.ann.term': 'all',
            'classifier.boost.m': 1,
            'classifier.boost.r': 0.95,
            'classifier.boost.t': 'real',
            'classifier.boost.w': 100,
            'classifier.dt.cat': 10,
            'classifier.dt.max': 10,
            'classifier.dt.min': 10,
            'classifier.dt.r': False,
            'classifier.dt.ra': 0.01,
            'classifier.dt.t': False,
            'classifier.knn.k': 32,
            'classifier.libsvm.c': 1,
            'classifier.libsvm.k': 'linear',
            'classifier.libsvm.m': 'csvc',
            'classifier.libsvm.nu': 0.5,
            'classifier.libsvm.opt': False,
            'classifier.libsvm.prob': False,
            'classifier.rf.acc': 0.01,
            'classifier.rf.cat': 10,
            'classifier.rf.max': 10,
            'classifier.rf.min': 10,
            'classifier.rf.nbtrees': 200,
            'classifier.rf.ra': 0,
            'classifier.rf.var': 0,
            'classifier.sharkkm.cstats': '',
            'classifier.sharkkm.incentroids': '',
            'classifier.sharkkm.k': 2,
            'classifier.sharkkm.maxiter': 10,
            'classifier.sharkrf.mtry': 0,
            'classifier.sharkrf.nbtrees': 100,
            'classifier.sharkrf.nodesize': 25,
            'classifier.sharkrf.oobr': 0.66,
            'feat': 'mean0, stdev0, min0, max0, mean1, stdev1, min1, max1, mean2, stdev2, min2, max2,mean3, stdev3, min3, max3, mean4, stdev4, min4, max4,mean5, stdev5, min5, max5,mean6, stdev6, min6, max6,mean7, stdev7, min7, max7,mean8, stdev8, min8, max8,mean9, stdev9, min9, max9,mean10, stdev10, min10, max10,\nmean11, stdev11, min11, max11,mean12, stdev12, min12, max12,mean13, stdev13, min13, max13,\n\n\n\n\n\n\n\n\n\n\n\n',
            'io.stats': '',
            'io.vd': outputs['Zonalstatistics']['out.vector.filename'],
            'layer': 0,
            'rand': 0,
            'v': True,
            'valid.layer': 0,
            'valid.vd': [],
            'io.confmatout': parameters['Confusion_matriccsv'],
            'io.out': parameters['Trained_mdlmdl']
        }
        outputs['Trainvectorclassifier'] = processing.run('otb:TrainVectorClassifier', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Confusion_matriccsv'] = outputs['Trainvectorclassifier']['io.confmatout']
        results['Trained_mdlmdl'] = outputs['Trainvectorclassifier']['io.out']
        return results

    def name(self):
        return 'Acquisition statistiques'

    def displayName(self):
        return 'Acquisition statistiques'

    def group(self):
        return 'Classification'

    def groupId(self):
        return ''

    def createInstance(self):
        return AcquisitionStatistiques()
