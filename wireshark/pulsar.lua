--[[

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

]]
--  [TOTAL_SIZE] [CMD_SIZE][CMD]
--  [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
local protobuf_dissector = Dissector.get("protobuf")
local pulsar_protocol = Proto("Pulsar", "pulsar.proto.BaseCommand")

-- Create protobuf dissector based on TCP.
-- The TCP dissector will parse tvb as pulsar format
-- @param name  The name of the new dissector.
-- @param desc  The description of the new dissector.
-- @param msgtype  Message type. This must be the root message defined in your .proto file.
local function get_pulsar_length(tvb, pinfo, tree)
    local totalLength = tvb(0, 4):uint()
    return totalLength + 4
end
local function dissect_pulsar_pdu(tvb, pinfo, tree)
    pinfo.cols.protocol = "Pulsar"
    local pduLength = tvb:len()
    local offset = 4
    local commandLenth = tvb(offset, 4):uint()
    offset = offset + 4
    local subtree = tree:add(pulsar_protocol, tvb())
    pinfo.private["pb_msg_type"] = "message,pulsar.proto.BaseCommand"
    pcall(Dissector.call, protobuf_dissector, tvb(offset, commandLenth):tvb(), pinfo, subtree)
    offset = offset + commandLenth
    if offset == pduLength then
        return
    end
    local checkSum = tvb(offset, 2):bytes():tohex()
    -- has not checksum ,but has payload
    if checkSum ~= '0E01' then
        local metaSize = tvb(offset, 4):uint()
        pinfo.private["pb_msg_type"] = "message,pulsar.proto.MessageMetadata"
        offset = offset + 4
        pcall(Dissector.call, protobuf_dissector, tvb(offset, metaSize):tvb(), pinfo, subtree)
        offset = offset + metaSize
    end
    -- has checksum , has payload
    if checkSum == '0E01' then
        offset = offset + 6 -- magic and checksum
        local metaSize = tvb(offset, 4):uint()
        pinfo.private["pb_msg_type"] = "message,pulsar.proto.MessageMetadata"
        offset = offset + 4
        pcall(Dissector.call, protobuf_dissector, tvb(offset, metaSize):tvb(), pinfo, subtree)
        offset = offset + metaSize
    end
end

pulsar_protocol.dissector = function(tvb, pinfo, tree)
    local bytes_consumed = 0
    dissect_tcp_pdus(tvb, tree, 4, get_pulsar_length, dissect_pulsar_pdu)
    bytes_consumed = tvb:len()
    return bytes_consumed
end

pulsar_protocol.prefs.port = Pref.uint("Pulsar TCP port", 6650)
local tcp_port = DissectorTable.get("tcp.port")
tcp_port:add(pulsar_protocol.prefs.port, pulsar_protocol)