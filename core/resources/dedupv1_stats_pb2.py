# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: dedupv1_stats.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)




DESCRIPTOR = _descriptor.FileDescriptor(
  name='dedupv1_stats.proto',
  package='',
  serialized_pb='\n\x13\x64\x65\x64upv1_stats.proto\"Y\n\x13\x43hunkIndexStatsData\x12 \n\x18imported_container_count\x18\x01 \x01(\x04\x12 \n\x18index_full_failure_count\x18\x02 \x01(\x04\"\xaa\x01\n\x13\x42lockIndexStatsData\x12\x18\n\x10index_read_count\x18\x01 \x01(\x04\x12\x19\n\x11index_write_count\x18\x02 \x01(\x04\x12\x1e\n\x16index_real_write_count\x18\x03 \x01(\x04\x12\x1c\n\x14imported_block_count\x18\x04 \x01(\x04\x12 \n\x18\x66\x61iled_block_write_count\x18\x05 \x01(\x04\"X\n\x13\x43hunkStoreStatsData\x12\x12\n\nread_count\x18\x01 \x01(\x04\x12\x13\n\x0bwrite_count\x18\x02 \x01(\x04\x12\x18\n\x10real_write_count\x18\x03 \x01(\x04\"a\n#ContainerStorageWriteCacheStatsData\x12\x11\n\thit_count\x18\x01 \x01(\x04\x12\x12\n\nmiss_count\x18\x02 \x01(\x04\x12\x13\n\x0b\x63heck_count\x18\x03 \x01(\x04\"\xb2\x02\n\x19\x43ontainerStorageStatsData\x12\x12\n\nread_count\x18\x01 \x01(\x04\x12\x1d\n\x15write_cache_hit_count\x18\x02 \x01(\x04\x12\x1f\n\x17\x63ontainer_timeout_count\x18\x03 \x01(\x04\x12\x1e\n\x16readed_container_count\x18\x04 \x01(\x04\x12!\n\x19\x63ommitted_container_count\x18\x05 \x01(\x04\x12\x1d\n\x15moved_container_count\x18\x06 \x01(\x04\x12\x1e\n\x16merged_container_count\x18\x07 \x01(\x04\x12\x1e\n\x16\x66\x61iled_container_count\x18\x08 \x01(\x04\x12\x1f\n\x17\x64\x65leted_container_count\x18\t \x01(\x04\"v\n\"ContainerStorageReadCacheStatsData\x12\x11\n\thit_count\x18\x01 \x01(\x04\x12\x12\n\nmiss_count\x18\x02 \x01(\x04\x12\x13\n\x0b\x63heck_count\x18\x03 \x01(\x04\x12\x14\n\x0cupdate_count\x18\x04 \x01(\x04\"V\n\x19\x42lockIndexFilterStatsData\x12\x11\n\thit_count\x18\x01 \x01(\x04\x12\x12\n\nmiss_count\x18\x02 \x01(\x04\x12\x12\n\nread_count\x18\x03 \x01(\x04\"f\n\x14\x42loomFilterStatsData\x12\x11\n\thit_count\x18\x01 \x01(\x04\x12\x12\n\nmiss_count\x18\x02 \x01(\x04\x12\x12\n\nread_count\x18\x03 \x01(\x04\x12\x13\n\x0bwrite_count\x18\x04 \x01(\x04\"W\n\x1a\x42yteCompareFilterStatsData\x12\x11\n\thit_count\x18\x01 \x01(\x04\x12\x12\n\nmiss_count\x18\x02 \x01(\x04\x12\x12\n\nread_count\x18\x03 \x01(\x04\"\x9e\x01\n\x19\x43hunkIndexFilterStatsData\x12\x11\n\thit_count\x18\x01 \x01(\x04\x12\x12\n\nmiss_count\x18\x02 \x01(\x04\x12\x12\n\nread_count\x18\x03 \x01(\x04\x12\x13\n\x0bwrite_count\x18\x04 \x01(\x04\x12\x15\n\rfailure_count\x18\x06 \x01(\x04\x12\x1a\n\x12\x65mpty_fp_hit_count\x18\x05 \x01(\x04\"\xc3\x01\n\x19GarbageCollectorStatsData\x12\x1d\n\x15processed_block_count\x18\x01 \x01(\x04\x12$\n\x1cprocessed_gc_candidate_count\x18\x02 \x01(\x04\x12\x1b\n\x13skipped_chunk_count\x18\x03 \x01(\x04\x12%\n\x1d\x61lready_processed_chunk_count\x18\x04 \x01(\x04\x12\x1d\n\x15processed_chunk_count\x18\x05 \x01(\x04\"o\n\x15RabinChunkerStatsData\x12\x13\n\x0b\x63hunk_count\x18\x01 \x01(\x04\x12\x1f\n\x17size_forced_chunk_count\x18\x02 \x01(\x04\x12 \n\x18\x63lose_forced_chunk_count\x18\x03 \x01(\x04\"i\n\x17\x43ontentStorageStatsData\x12\x12\n\nread_count\x18\x01 \x01(\x04\x12\x13\n\x0bwrite_count\x18\x02 \x01(\x04\x12\x11\n\tread_size\x18\x03 \x01(\x04\x12\x12\n\nwrite_size\x18\x04 \x01(\x04\"\xa5\x01\n\x0cLogStatsData\x12\x13\n\x0b\x65vent_count\x18\x01 \x01(\x04\x12\x1c\n\x14replayed_event_count\x18\x02 \x01(\x04\x12\x33\n\rlogtype_count\x18\x03 \x03(\x0b\x32\x1c.LogStatsData.LogTypeCounter\x1a-\n\x0eLogTypeCounter\x12\x0c\n\x04type\x18\x01 \x01(\x05\x12\r\n\x05\x63ount\x18\x02 \x01(\x04')




_CHUNKINDEXSTATSDATA = _descriptor.Descriptor(
  name='ChunkIndexStatsData',
  full_name='ChunkIndexStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='imported_container_count', full_name='ChunkIndexStatsData.imported_container_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='index_full_failure_count', full_name='ChunkIndexStatsData.index_full_failure_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=23,
  serialized_end=112,
)


_BLOCKINDEXSTATSDATA = _descriptor.Descriptor(
  name='BlockIndexStatsData',
  full_name='BlockIndexStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='index_read_count', full_name='BlockIndexStatsData.index_read_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='index_write_count', full_name='BlockIndexStatsData.index_write_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='index_real_write_count', full_name='BlockIndexStatsData.index_real_write_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='imported_block_count', full_name='BlockIndexStatsData.imported_block_count', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='failed_block_write_count', full_name='BlockIndexStatsData.failed_block_write_count', index=4,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=115,
  serialized_end=285,
)


_CHUNKSTORESTATSDATA = _descriptor.Descriptor(
  name='ChunkStoreStatsData',
  full_name='ChunkStoreStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='read_count', full_name='ChunkStoreStatsData.read_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='write_count', full_name='ChunkStoreStatsData.write_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='real_write_count', full_name='ChunkStoreStatsData.real_write_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=287,
  serialized_end=375,
)


_CONTAINERSTORAGEWRITECACHESTATSDATA = _descriptor.Descriptor(
  name='ContainerStorageWriteCacheStatsData',
  full_name='ContainerStorageWriteCacheStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hit_count', full_name='ContainerStorageWriteCacheStatsData.hit_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='miss_count', full_name='ContainerStorageWriteCacheStatsData.miss_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='check_count', full_name='ContainerStorageWriteCacheStatsData.check_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=377,
  serialized_end=474,
)


_CONTAINERSTORAGESTATSDATA = _descriptor.Descriptor(
  name='ContainerStorageStatsData',
  full_name='ContainerStorageStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='read_count', full_name='ContainerStorageStatsData.read_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='write_cache_hit_count', full_name='ContainerStorageStatsData.write_cache_hit_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='container_timeout_count', full_name='ContainerStorageStatsData.container_timeout_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='readed_container_count', full_name='ContainerStorageStatsData.readed_container_count', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='committed_container_count', full_name='ContainerStorageStatsData.committed_container_count', index=4,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='moved_container_count', full_name='ContainerStorageStatsData.moved_container_count', index=5,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='merged_container_count', full_name='ContainerStorageStatsData.merged_container_count', index=6,
      number=7, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='failed_container_count', full_name='ContainerStorageStatsData.failed_container_count', index=7,
      number=8, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='deleted_container_count', full_name='ContainerStorageStatsData.deleted_container_count', index=8,
      number=9, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=477,
  serialized_end=783,
)


_CONTAINERSTORAGEREADCACHESTATSDATA = _descriptor.Descriptor(
  name='ContainerStorageReadCacheStatsData',
  full_name='ContainerStorageReadCacheStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hit_count', full_name='ContainerStorageReadCacheStatsData.hit_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='miss_count', full_name='ContainerStorageReadCacheStatsData.miss_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='check_count', full_name='ContainerStorageReadCacheStatsData.check_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='update_count', full_name='ContainerStorageReadCacheStatsData.update_count', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=785,
  serialized_end=903,
)


_BLOCKINDEXFILTERSTATSDATA = _descriptor.Descriptor(
  name='BlockIndexFilterStatsData',
  full_name='BlockIndexFilterStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hit_count', full_name='BlockIndexFilterStatsData.hit_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='miss_count', full_name='BlockIndexFilterStatsData.miss_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='read_count', full_name='BlockIndexFilterStatsData.read_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=905,
  serialized_end=991,
)


_BLOOMFILTERSTATSDATA = _descriptor.Descriptor(
  name='BloomFilterStatsData',
  full_name='BloomFilterStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hit_count', full_name='BloomFilterStatsData.hit_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='miss_count', full_name='BloomFilterStatsData.miss_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='read_count', full_name='BloomFilterStatsData.read_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='write_count', full_name='BloomFilterStatsData.write_count', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=993,
  serialized_end=1095,
)


_BYTECOMPAREFILTERSTATSDATA = _descriptor.Descriptor(
  name='ByteCompareFilterStatsData',
  full_name='ByteCompareFilterStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hit_count', full_name='ByteCompareFilterStatsData.hit_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='miss_count', full_name='ByteCompareFilterStatsData.miss_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='read_count', full_name='ByteCompareFilterStatsData.read_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1097,
  serialized_end=1184,
)


_CHUNKINDEXFILTERSTATSDATA = _descriptor.Descriptor(
  name='ChunkIndexFilterStatsData',
  full_name='ChunkIndexFilterStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hit_count', full_name='ChunkIndexFilterStatsData.hit_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='miss_count', full_name='ChunkIndexFilterStatsData.miss_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='read_count', full_name='ChunkIndexFilterStatsData.read_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='write_count', full_name='ChunkIndexFilterStatsData.write_count', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='failure_count', full_name='ChunkIndexFilterStatsData.failure_count', index=4,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='empty_fp_hit_count', full_name='ChunkIndexFilterStatsData.empty_fp_hit_count', index=5,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1187,
  serialized_end=1345,
)


_GARBAGECOLLECTORSTATSDATA = _descriptor.Descriptor(
  name='GarbageCollectorStatsData',
  full_name='GarbageCollectorStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='processed_block_count', full_name='GarbageCollectorStatsData.processed_block_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='processed_gc_candidate_count', full_name='GarbageCollectorStatsData.processed_gc_candidate_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='skipped_chunk_count', full_name='GarbageCollectorStatsData.skipped_chunk_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='already_processed_chunk_count', full_name='GarbageCollectorStatsData.already_processed_chunk_count', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='processed_chunk_count', full_name='GarbageCollectorStatsData.processed_chunk_count', index=4,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1348,
  serialized_end=1543,
)


_RABINCHUNKERSTATSDATA = _descriptor.Descriptor(
  name='RabinChunkerStatsData',
  full_name='RabinChunkerStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='chunk_count', full_name='RabinChunkerStatsData.chunk_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='size_forced_chunk_count', full_name='RabinChunkerStatsData.size_forced_chunk_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='close_forced_chunk_count', full_name='RabinChunkerStatsData.close_forced_chunk_count', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1545,
  serialized_end=1656,
)


_CONTENTSTORAGESTATSDATA = _descriptor.Descriptor(
  name='ContentStorageStatsData',
  full_name='ContentStorageStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='read_count', full_name='ContentStorageStatsData.read_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='write_count', full_name='ContentStorageStatsData.write_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='read_size', full_name='ContentStorageStatsData.read_size', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='write_size', full_name='ContentStorageStatsData.write_size', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1658,
  serialized_end=1763,
)


_LOGSTATSDATA_LOGTYPECOUNTER = _descriptor.Descriptor(
  name='LogTypeCounter',
  full_name='LogStatsData.LogTypeCounter',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='LogStatsData.LogTypeCounter.type', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='count', full_name='LogStatsData.LogTypeCounter.count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1886,
  serialized_end=1931,
)

_LOGSTATSDATA = _descriptor.Descriptor(
  name='LogStatsData',
  full_name='LogStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='event_count', full_name='LogStatsData.event_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='replayed_event_count', full_name='LogStatsData.replayed_event_count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='logtype_count', full_name='LogStatsData.logtype_count', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_LOGSTATSDATA_LOGTYPECOUNTER, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1766,
  serialized_end=1931,
)

_LOGSTATSDATA_LOGTYPECOUNTER.containing_type = _LOGSTATSDATA;
_LOGSTATSDATA.fields_by_name['logtype_count'].message_type = _LOGSTATSDATA_LOGTYPECOUNTER
DESCRIPTOR.message_types_by_name['ChunkIndexStatsData'] = _CHUNKINDEXSTATSDATA
DESCRIPTOR.message_types_by_name['BlockIndexStatsData'] = _BLOCKINDEXSTATSDATA
DESCRIPTOR.message_types_by_name['ChunkStoreStatsData'] = _CHUNKSTORESTATSDATA
DESCRIPTOR.message_types_by_name['ContainerStorageWriteCacheStatsData'] = _CONTAINERSTORAGEWRITECACHESTATSDATA
DESCRIPTOR.message_types_by_name['ContainerStorageStatsData'] = _CONTAINERSTORAGESTATSDATA
DESCRIPTOR.message_types_by_name['ContainerStorageReadCacheStatsData'] = _CONTAINERSTORAGEREADCACHESTATSDATA
DESCRIPTOR.message_types_by_name['BlockIndexFilterStatsData'] = _BLOCKINDEXFILTERSTATSDATA
DESCRIPTOR.message_types_by_name['BloomFilterStatsData'] = _BLOOMFILTERSTATSDATA
DESCRIPTOR.message_types_by_name['ByteCompareFilterStatsData'] = _BYTECOMPAREFILTERSTATSDATA
DESCRIPTOR.message_types_by_name['ChunkIndexFilterStatsData'] = _CHUNKINDEXFILTERSTATSDATA
DESCRIPTOR.message_types_by_name['GarbageCollectorStatsData'] = _GARBAGECOLLECTORSTATSDATA
DESCRIPTOR.message_types_by_name['RabinChunkerStatsData'] = _RABINCHUNKERSTATSDATA
DESCRIPTOR.message_types_by_name['ContentStorageStatsData'] = _CONTENTSTORAGESTATSDATA
DESCRIPTOR.message_types_by_name['LogStatsData'] = _LOGSTATSDATA

class ChunkIndexStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CHUNKINDEXSTATSDATA

  # @@protoc_insertion_point(class_scope:ChunkIndexStatsData)

class BlockIndexStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _BLOCKINDEXSTATSDATA

  # @@protoc_insertion_point(class_scope:BlockIndexStatsData)

class ChunkStoreStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CHUNKSTORESTATSDATA

  # @@protoc_insertion_point(class_scope:ChunkStoreStatsData)

class ContainerStorageWriteCacheStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CONTAINERSTORAGEWRITECACHESTATSDATA

  # @@protoc_insertion_point(class_scope:ContainerStorageWriteCacheStatsData)

class ContainerStorageStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CONTAINERSTORAGESTATSDATA

  # @@protoc_insertion_point(class_scope:ContainerStorageStatsData)

class ContainerStorageReadCacheStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CONTAINERSTORAGEREADCACHESTATSDATA

  # @@protoc_insertion_point(class_scope:ContainerStorageReadCacheStatsData)

class BlockIndexFilterStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _BLOCKINDEXFILTERSTATSDATA

  # @@protoc_insertion_point(class_scope:BlockIndexFilterStatsData)

class BloomFilterStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _BLOOMFILTERSTATSDATA

  # @@protoc_insertion_point(class_scope:BloomFilterStatsData)

class ByteCompareFilterStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _BYTECOMPAREFILTERSTATSDATA

  # @@protoc_insertion_point(class_scope:ByteCompareFilterStatsData)

class ChunkIndexFilterStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CHUNKINDEXFILTERSTATSDATA

  # @@protoc_insertion_point(class_scope:ChunkIndexFilterStatsData)

class GarbageCollectorStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GARBAGECOLLECTORSTATSDATA

  # @@protoc_insertion_point(class_scope:GarbageCollectorStatsData)

class RabinChunkerStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RABINCHUNKERSTATSDATA

  # @@protoc_insertion_point(class_scope:RabinChunkerStatsData)

class ContentStorageStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CONTENTSTORAGESTATSDATA

  # @@protoc_insertion_point(class_scope:ContentStorageStatsData)

class LogStatsData(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType

  class LogTypeCounter(_message.Message):
    __metaclass__ = _reflection.GeneratedProtocolMessageType
    DESCRIPTOR = _LOGSTATSDATA_LOGTYPECOUNTER

    # @@protoc_insertion_point(class_scope:LogStatsData.LogTypeCounter)
  DESCRIPTOR = _LOGSTATSDATA

  # @@protoc_insertion_point(class_scope:LogStatsData)


# @@protoc_insertion_point(module_scope)
