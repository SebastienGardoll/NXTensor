from typing import NewType, Sequence, Mapping, Union, Tuple

from nxtensor.utils.coordinates import Coordinate
from nxtensor.utils.time_resolutions import TimeResolution

VariableId = str
LabelId = str

# A block of extraction metadata (lat, lon, year, month, etc.).
MetaDataBlock = NewType('MetaDataBlock', Sequence[Mapping[Union[TimeResolution, Coordinate], Union[int, float, str]]])


# A Period is a tuple composed of values that correspond to the values of
# TimeResolution::TIME_RESOLUTION_KEYS (same order).
Period = NewType('Period', Tuple[Union[float, int], ...])
