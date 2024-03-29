import numpy as np
from numpy import float32
from pyspark.ml import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType 
import pyspark.sql.functions as F
from typing import Iterable


IDENTIFIERS = ['TransactionID']
TARGET_COLUMN = ['isFraud']
TIME_COLUMNS = ['TransactionDT']

## Binary columnes passed Chi2 test (EDA)
BINARY_FEATURES = ['V286', 'V26']

## Categorical columns passed Chi2 test (EDA)
CATEGORICAL_FEATURES = []

## Discrete columnes passed Chi2 test (EDA)
DISCRETE_FEATURES = ['weekdays', 'minutes']

## Continuous columns with correlation < 0.8 (EDA)
CONTINUOUS_FEATURES = [
    'addr1', 'addr2', 'C7', 'V97', 'V183', 'V236', 'V279', 
    'V280', 'V290', 'V306', 'V308', 'V317'
    ]
max_thresholds = {
    'addr1': 540.0,
    'addr2': 87.0,
    'C7': 10.0,
    'V97': 7.0,
    'V183': 4.0,
    'V236': 3.0,
    'V279': 5.0,
    'V280': 9.0,
    'V290': 3.0,
    'V306': 938.0,
    'V308': 1649.5,
    'V317': 1762.0
    }
# 0, >0
binary_1 = ['V26', 'V286']


class DiscreteToBinaryTransformer0(Transformer):
    """
    Consolidates discrete variables to groups 0, >0. 
    """
    def __init__(self, binary_1: Iterable[str]):
        super(DiscreteToBinaryTransformer0, self).__init__()
        self.binary_1 = binary_1   


    def _transform(self, df: DataFrame) -> DataFrame:
        for var in self.binary_1:
            df = df.withColumn(
                var, F.when(F.col(var) > 0, 1).otherwise(F.col(var))
                )
        return df


class ContinuousOutliersCapper(Transformer):
    """  
    Caps max values of continuous variables by 99th percentile values if 
    the difference between max value and 99th percentile value exceeds 
    standard deviation.  
    """
    def __init__(self, max_thresholds: dict):
        super(ContinuousOutliersCapper, self).__init__()
        self.maximum = max_thresholds


    def _transform(self, df: DataFrame) -> DataFrame:
        for k, v in self.maximum.items():
            df = df.withColumn(
                k, F.when(
                    F.isnull(F.col(k)), F.col(k)
                    ).otherwise(F.least(F.col(k), F.lit(v)))
          )
        return df


class TimeFeaturesGenerator(Transformer):
    """
    A custom Transformer which generates weekdays, hours and minutes from time 
    variable.
    """

    def __init__(self, time_var: str):
        super(TimeFeaturesGenerator, self).__init__()
        self.time_var = time_var

    def _transform(self, df: DataFrame) -> DataFrame:
        w = 60 * 60 * 24 * 7
        d = 60 * 60 * 24
        h = 60 * 60
        m = 60 
        time_var = self.time_var 
        df = df.withColumn(
            'weekdays', (F.col(time_var) % w / d).cast(IntegerType())
            )
        df = df.withColumn(
            'hours', (F.col(time_var) % d / h).cast(IntegerType())
            )
        df = df.withColumn(
            'minutes', (F.col(time_var) % d % h / m).cast(IntegerType())
            )
        return df


class FillNan(Transformer):
    """
    A custom Transformer fills -999 for null in binary and discrete features
    and 0 in continuous features
    """

    def __init__(self):
        super(FillNan, self).__init__()


    def _transform(self, df: DataFrame) -> DataFrame:
        df = df.na.fill(value=-999, subset=BINARY_FEATURES)
        df = df.na.fill(value=-999, subset=DISCRETE_FEATURES)
        df = df.na.fill(value=0, subset=CONTINUOUS_FEATURES)
        return df


class StringFromDiscrete(Transformer):
    """
    Transforms discrete variables to string format (for one-hot encoding).
    """

    def __init__(self, var_list: Iterable[str]):
        super(StringFromDiscrete, self).__init__()
        self.var_list = var_list


    def _transform(self, df: DataFrame) -> DataFrame:
        for var in self.var_list:
            df = df.withColumn(var + '_str', df[var].cast(StringType()))      
        return df