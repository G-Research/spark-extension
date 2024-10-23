from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Diff(_message.Message):
    __slots__ = ["left", "right", "options", "idColumn", "ignoreColumn"]
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    IDCOLUMN_FIELD_NUMBER: _ClassVar[int]
    IGNORECOLUMN_FIELD_NUMBER: _ClassVar[int]
    left: bytes
    right: bytes
    options: DiffOptions
    idColumn: _containers.RepeatedScalarFieldContainer[str]
    ignoreColumn: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, left: _Optional[bytes] = ..., right: _Optional[bytes] = ..., options: _Optional[_Union[DiffOptions, _Mapping]] = ..., idColumn: _Optional[_Iterable[str]] = ..., ignoreColumn: _Optional[_Iterable[str]] = ...) -> None: ...

class DiffOptions(_message.Message):
    __slots__ = ["diffColumn", "leftColumnPrefix", "rightColumnPrefix", "insertDiffValue", "changeDiffValue", "deleteDiffValue", "nochangeDiffValue", "changeColumn", "diffMode", "sparseMode", "defaultComparator", "dataTypeComparators", "columnNameComparators"]
    class DataTypeComparatorsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Comparator
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Comparator, _Mapping]] = ...) -> None: ...
    class ColumnNameComparatorsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Comparator
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Comparator, _Mapping]] = ...) -> None: ...
    DIFFCOLUMN_FIELD_NUMBER: _ClassVar[int]
    LEFTCOLUMNPREFIX_FIELD_NUMBER: _ClassVar[int]
    RIGHTCOLUMNPREFIX_FIELD_NUMBER: _ClassVar[int]
    INSERTDIFFVALUE_FIELD_NUMBER: _ClassVar[int]
    CHANGEDIFFVALUE_FIELD_NUMBER: _ClassVar[int]
    DELETEDIFFVALUE_FIELD_NUMBER: _ClassVar[int]
    NOCHANGEDIFFVALUE_FIELD_NUMBER: _ClassVar[int]
    CHANGECOLUMN_FIELD_NUMBER: _ClassVar[int]
    DIFFMODE_FIELD_NUMBER: _ClassVar[int]
    SPARSEMODE_FIELD_NUMBER: _ClassVar[int]
    DEFAULTCOMPARATOR_FIELD_NUMBER: _ClassVar[int]
    DATATYPECOMPARATORS_FIELD_NUMBER: _ClassVar[int]
    COLUMNNAMECOMPARATORS_FIELD_NUMBER: _ClassVar[int]
    diffColumn: str
    leftColumnPrefix: str
    rightColumnPrefix: str
    insertDiffValue: str
    changeDiffValue: str
    deleteDiffValue: str
    nochangeDiffValue: str
    changeColumn: str
    diffMode: str
    sparseMode: str
    defaultComparator: Comparator
    dataTypeComparators: _containers.MessageMap[str, Comparator]
    columnNameComparators: _containers.MessageMap[str, Comparator]
    def __init__(self, diffColumn: _Optional[str] = ..., leftColumnPrefix: _Optional[str] = ..., rightColumnPrefix: _Optional[str] = ..., insertDiffValue: _Optional[str] = ..., changeDiffValue: _Optional[str] = ..., deleteDiffValue: _Optional[str] = ..., nochangeDiffValue: _Optional[str] = ..., changeColumn: _Optional[str] = ..., diffMode: _Optional[str] = ..., sparseMode: _Optional[str] = ..., defaultComparator: _Optional[_Union[Comparator, _Mapping]] = ..., dataTypeComparators: _Optional[_Mapping[str, Comparator]] = ..., columnNameComparators: _Optional[_Mapping[str, Comparator]] = ...) -> None: ...

class Comparator(_message.Message):
    __slots__ = ["default", "nullSafeEqual", "equiv", "epsilon", "string", "duration", "map"]
    class DefaultComparator(_message.Message):
        __slots__ = []
        def __init__(self) -> None: ...
    class NullSafeComparator(_message.Message):
        __slots__ = []
        def __init__(self) -> None: ...
    class EquivComparator(_message.Message):
        __slots__ = ["equiv", "dataType"]
        EQUIV_FIELD_NUMBER: _ClassVar[int]
        DATATYPE_FIELD_NUMBER: _ClassVar[int]
        equiv: str
        dataType: str
        def __init__(self, equiv: _Optional[str] = ..., dataType: _Optional[str] = ...) -> None: ...
    class EpsilonComparator(_message.Message):
        __slots__ = ["epsilon", "relative", "inclusive"]
        EPSILON_FIELD_NUMBER: _ClassVar[int]
        RELATIVE_FIELD_NUMBER: _ClassVar[int]
        INCLUSIVE_FIELD_NUMBER: _ClassVar[int]
        epsilon: float
        relative: bool
        inclusive: bool
        def __init__(self, epsilon: _Optional[float] = ..., relative: bool = ..., inclusive: bool = ...) -> None: ...
    class StringComparator(_message.Message):
        __slots__ = ["whitespaceAgnostic"]
        WHITESPACEAGNOSTIC_FIELD_NUMBER: _ClassVar[int]
        whitespaceAgnostic: bool
        def __init__(self, whitespaceAgnostic: bool = ...) -> None: ...
    class DurationComparator(_message.Message):
        __slots__ = ["duration"]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        duration: str
        def __init__(self, duration: _Optional[str] = ...) -> None: ...
    class MapComparator(_message.Message):
        __slots__ = ["keyDataType", "valueDataType", "keyOrderSensitive"]
        KEYDATATYPE_FIELD_NUMBER: _ClassVar[int]
        VALUEDATATYPE_FIELD_NUMBER: _ClassVar[int]
        KEYORDERSENSITIVE_FIELD_NUMBER: _ClassVar[int]
        keyDataType: str
        valueDataType: str
        keyOrderSensitive: bool
        def __init__(self, keyDataType: _Optional[str] = ..., valueDataType: _Optional[str] = ..., keyOrderSensitive: bool = ...) -> None: ...
    DEFAULT_FIELD_NUMBER: _ClassVar[int]
    NULLSAFEEQUAL_FIELD_NUMBER: _ClassVar[int]
    EQUIV_FIELD_NUMBER: _ClassVar[int]
    EPSILON_FIELD_NUMBER: _ClassVar[int]
    STRING_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    MAP_FIELD_NUMBER: _ClassVar[int]
    default: Comparator.DefaultComparator
    nullSafeEqual: Comparator.NullSafeComparator
    equiv: Comparator.EquivComparator
    epsilon: Comparator.EpsilonComparator
    string: Comparator.StringComparator
    duration: Comparator.DurationComparator
    map: Comparator.MapComparator
    def __init__(self, default: _Optional[_Union[Comparator.DefaultComparator, _Mapping]] = ..., nullSafeEqual: _Optional[_Union[Comparator.NullSafeComparator, _Mapping]] = ..., equiv: _Optional[_Union[Comparator.EquivComparator, _Mapping]] = ..., epsilon: _Optional[_Union[Comparator.EpsilonComparator, _Mapping]] = ..., string: _Optional[_Union[Comparator.StringComparator, _Mapping]] = ..., duration: _Optional[_Union[Comparator.DurationComparator, _Mapping]] = ..., map: _Optional[_Union[Comparator.MapComparator, _Mapping]] = ...) -> None: ...
