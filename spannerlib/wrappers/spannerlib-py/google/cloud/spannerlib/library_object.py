import logging

from .internal.spannerlib import Spannerlib

logger = logging.getLogger(__name__)


class AbstractLibraryObject:
    def __init__(self, id):
        self._id = id

    @property
    def id(self):
        return self._id

    def release(self):
        try:
            lib = Spannerlib.get_instance().lib
            if lib:
                lib.Release(self._id)
        except Exception as e:
            logger.warning(f"Error releasing pinnerId {self._id}: {e}")
