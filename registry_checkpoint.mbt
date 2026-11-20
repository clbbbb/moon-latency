///|
pub(all) enum RegistryCheckpointError {
  RegistryHeader
  RegistryFrame(Int32)
  RegistryName(Int32)
  RegistryQuota(Int32)
  ServiceCheckpoint(Int32, CheckpointError)
}

///|
/// MLR1 serializes each configured service in name order.
pub fn Registry::checkpoint(self : Registry) -> Bytes {
  let buffer = @buffer.Buffer::Buffer()
  buffer.write_bytes(b"MLR1"[:])
  buffer.write_uint16_be(self.capacity.to_uint16())
  let names = self.names()
  buffer.write_uint16_be(names.length().to_uint16())
  for name in names {
    buffer.write_byte(name.length().to_byte())
    for ch in name {
      buffer.write_byte(ch.to_int().to_byte())
    }
    let bytes = self.services.get(name).unwrap().checkpoint()
    append_dword(buffer, bytes.length().reinterpret_as_uint())
    buffer.write_bytes(bytes[:])
  }
  buffer.to_bytes()
}

///|
fn ascii_name(bytes : BytesView) -> String? {
  let result = StringBuilder()
  for byte in bytes {
    if byte.to_int() > 127 {
      return None
    }
    result.write_char(byte.to_int().unsafe_to_char())
  }
  let name = result.to_string()
  if valid_service_name(name) {
    Some(name)
  } else {
    None
  }
}

///|
pub fn Registry::restore(
  bytes : Bytes,
) -> Result[Registry, RegistryCheckpointError] {
  let length = bytes.length()
  if length < 8 || length > 80000000 || bytes[:4].to_owned() != b"MLR1" {
    return Err(RegistryHeader)
  }
  let capacity = bytes.unsafe_read_uint16_be(4).to_int()
  let count = bytes.unsafe_read_uint16_be(6).to_int()
  if capacity < 1 || capacity > 256 || count > capacity {
    return Err(RegistryHeader)
  }
  let registry = Registry::new(capacity~).unwrap()
  let mut offset = 8
  let mut previous = ""
  for index in 0..<count {
    if length - offset < 5 {
      return Err(RegistryFrame(index))
    }
    let name_length = bytes[offset].to_int()
    offset += 1
    if name_length < 1 || name_length > 64 || name_length > length - offset - 4 {
      return Err(RegistryName(index))
    }
    let name = match ascii_name(bytes[offset:offset + name_length]) {
      Some(name) if name > previous => name
      _ => return Err(RegistryName(index))
    }
    previous = name
    offset += name_length
    let size = bytes.unsafe_read_uint32_be(offset)
    offset += 4
    if size < 27U || size > (length - offset).reinterpret_as_uint() {
      return Err(RegistryFrame(index))
    }
    let end = offset + size.reinterpret_as_int()
    let slots = bytes.unsafe_read_uint16_be(offset + 21).to_int()
    let identities = bytes.unsafe_read_uint16_be(offset + 23).to_int()
    if registry.reserved_slots + slots > 4096 ||
      registry.reserved_identities + identities > 8192 {
      return Err(RegistryQuota(index))
    }
    let collector = match Collector::restore(bytes[offset:end].to_owned()) {
      Ok(value) => value
      Err(error) => return Err(ServiceCheckpoint(index, error))
    }
    registry.services[name] = collector
    registry.reserved_slots += slots
    registry.reserved_identities += identities
    offset = end
  }
  if offset != length {
    return Err(RegistryFrame(count))
  }
  Ok(registry)
}
