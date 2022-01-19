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
do
    local protobuf_dissector = Dissector.get("protobuf")

    -- Create protobuf dissector based on UDP or TCP.
    -- The UDP dissector will take the whole tvb as a message.
    -- The TCP dissector will parse tvb as format:
    --         [4bytes length][a message][4bytes length][a message]...
    -- @param name  The name of the new dissector.
    -- @param desc  The description of the new dissector.
    -- @param for_udp  Register the new dissector to UDP table.(Enable 'Decode as')
    -- @param for_tcp  Register the new dissector to TCP table.(Enable 'Decode as')
    -- @param msgtype  Message type. This must be the root message defined in your .proto file.
    local function create_protobuf_dissector(name, desc, for_udp, for_tcp, msgtype)
        local proto = Proto(name, desc)

        local f_proto = ProtoField.bytes(name .. ".PayLoad", "PayLoad", base.Dec)
        local f_length = ProtoField.uint32(name .. ".length", "Length", base.DEC)
        proto.fields = {f_length, f_proto}

        proto.dissector = function(tvb, pinfo, tree)
            local subtree = tree:add(proto, tvb())
            if for_udp and pinfo.port_type == 3 then -- UDP
                if msgtype ~= nil then
                    pinfo.private["pb_msg_type"] = "message," .. msgtype
                end
                pcall(Dissector.call, protobuf_dissector, tvb, pinfo, subtree)
            elseif for_tcp and pinfo.port_type == 2 then -- TCP
                if 4 > tvb:len() then
                    return
                end
                local offset = 0
                local totalLength = tvb(offset, 4):uint()
                if totalLength + 4 > tvb:len() then
                    return
                end
                offset = offset + 4
                local commandLenth = tvb(offset, 4):uint()
                offset = offset + 4
                if msgtype ~= nil then
                    pinfo.private["pb_msg_type"] = "message," .. msgtype
                end
                pcall(Dissector.call, protobuf_dissector, tvb(offset, commandLenth):tvb(), pinfo, subtree)
                offset = offset + commandLenth
                if totalLength > commandLenth + 4 + 4 then
                    local checkSum = tvb(offset, 2):bytes():tohex()
                    -- has not checksum ,but has payload
                    if checkSum ~= '0E01' then
                        print(checkSum)
                        local metaSize = tvb(offset, 4):uint()
                        print(metaSize)
                        print(tvb(offset, metaSize):tvb())
                        pinfo.private["pb_msg_type"] = "message,pulsar.proto.MessageMetadata"
                        offset = offset + 4
                        pcall(Dissector.call, protobuf_dissector, tvb(offset, metaSize):tvb(), pinfo, subtree)
                        offset = offset + metaSize
                        subtree:add(f_proto, tvb(offset, totalLength - offset))
                    end
                    -- has checksum , has payload
                    if checkSum == '0E01' then
                        offset = offset + 6 -- magic and checksum
                        local metaSize = tvb(offset, 4):uint()
                        pinfo.private["pb_msg_type"] = "message,pulsar.proto.MessageMetadata"
                        offset = offset + 4
                        pcall(Dissector.call, protobuf_dissector, tvb(offset, metaSize):tvb(), pinfo, subtree)
                        offset = offset + metaSize
                        subtree:add(f_proto, tvb(offset, totalLength - offset + 4))
                    end

                end
            end
            pinfo.columns.protocol:set(name)
        end

        if for_udp then
            DissectorTable.get("udp.port"):add(0, proto)
        end
        if for_tcp then
            DissectorTable.get("tcp.port"):add(0, proto)
        end
        return proto
    end

    -- default pure protobuf udp and tcp dissector without message type
    create_protobuf_dissector("protobuf_udp", "Protobuf UDP")
    create_protobuf_dissector("protobuf_tcp", "Protobuf TCP")
    -- add more protobuf dissectors with message types
    pulsar = create_protobuf_dissector("Pulsar", "pulsar.proto.BaseCommand", true, true, "pulsar.proto.BaseCommand")

    -- register our dissector upon tcp port 6650 (default)
    pulsar.prefs.port = Pref.uint("Pulsar TCP port", 6650)
    local tcp_port = DissectorTable.get("tcp.port")
    tcp_port:add(pulsar.prefs.port, pulsar)
end
