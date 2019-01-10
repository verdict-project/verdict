from abc import ABCMeta, abstractmethod

class DatatypeConverterBase(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def read_value(java_obj, index, col_type):
        raise NotImplementedError

