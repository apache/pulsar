#!/usr/bin/env python

import argparse
import traceback

from kazoo.client import KazooClient

from mltool import MLDataFormats_pb2

'''
This util provides api to access managed-ledger data if it's ONLY stored in binary format in to zookeeper.
It also provides command line tool-access to execute these commands.

Run: run from parent directory
python -m mltool.ManagedLedgerAdminTool
'''
managedLedgerPath = "/managed-ledgers/"
printMlCommand = "print-ml"
deleteMlLedgerIds = "delete-ml-ledgers"
printCursorsCommands = "print-cursor"
updateMakDeleteCursor = "update-mark-delete-cursor"

'''
Returns managed-ledger info for given managed-leger path 
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
''' 
def getManagedLedgerInfo(zk, mlPath):
    try:
        # get managed-ledger info
        mlData = zk.get(mlPath)[0]
        mlInfo = MLDataFormats_pb2.ManagedLedgerInfo()
        mlInfo.ParseFromString(mlData)
        return mlInfo
    except Exception as e:
            traceback.print_exc()
            print 'Failed to get data for {} due to {}'.format(mlPath, repr(e))


'''
Delete specific ledgerIds from the managed-ledger info and updates into zk
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
deleteLedgerIds : str
    comma separated deleting ledger-ids (eg: 123,124)
'''  
def deleteLedgerIdsFromManagedLedgerInfo(zk, mlPath, deletLedgerIds):
    try:
        # get managed-ledger info
        mlData = zk.get(mlPath)[0]
        mlInfo = MLDataFormats_pb2.ManagedLedgerInfo()
        mlInfo.ParseFromString(mlData)
        ledgerInfoList = mlInfo.ledgerInfo
        
        for ledgerInfo in ledgerInfoList:
            print ledgerInfo.ledgerId
            if ledgerInfo.ledgerId in deletLedgerIds:
                ledgerInfoList.remove(ledgerInfo)
                updatedMlInfo = mlInfo.SerializeToString();
                zk.set(mlPath, updatedMlInfo, -1)
                print 'Updated {} with value\n{}'.format(mlPath, str(mlInfo))
        
    except Exception as e:
            traceback.print_exc()
            print 'Failed to delete ledgerIds for {} due to {}'.format(mlPath, repr(e))

'''
Returns managed-ledger cursor info for given managed-cursor path
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
cursorName : str
    managed-cursor path
'''
def getManagedCursorInfo(zk, mlPath):
    try:
        cursors = zk.get_children(mlPath)
        cursorList = {}
        for cursor in cursors:
            cursorData = zk.get(mlPath + "/" + cursor)[0]
            cursorInfo = MLDataFormats_pb2.ManagedCursorInfo()
            cursorInfo.ParseFromString(cursorData)
            cursorList[cursor] = cursorInfo
        return cursorList
    except Exception as e:
            traceback.print_exc()
            print 'Failed to get ml-cursor {} due to {}'.format(mlPath, repr(e))


'''
Update mark-delete position of the given managed-cursor into zk
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
cursorName : str
    managed-cursor path
markDeletePosition: str
    markDeletePosition combination of <ledgerId>:<entryId> (eg. 123:1)
''' 
def updateCursorMarkDelete(zk, cursorPath, markDeleteLedgerId, markDeleteEntryId):
    try:
        cursorData = zk.get(cursorPath)[0]
        cursorInfo = MLDataFormats_pb2.ManagedCursorInfo()
        cursorInfo.ParseFromString(cursorData)
        cursorInfo.markDeleteLedgerId = markDeleteLedgerId
        cursorInfo.markDeleteEntryId = markDeleteEntryId
        sData = cursorInfo.SerializeToString()
        zk.set(cursorPath, sData, -1)
        print 'Updated {} with value \n{}'.format(cursorPath, cursorInfo)
    except Exception as e:
            traceback.print_exc()
            print 'Failed to update ml-cursor {} due to {}'.format(cursorPath, repr(e))



'''
print managed-ledger info for given managed-leger path 
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
    
eg: 
--zkServer localhost:2181 --mlPath sample/standalone/ns1/persistent/test --command print-ml
''' 
def printManagedLedgerCommand(zk, mlPath):
    print getManagedLedgerInfo(zk, mlPath)
    

'''
print managed-ledger cursor info for given managed-cursor path
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
cursorName : str
    managed-cursor path

eg:
--zkServer localhost:2181 --mlPath sample/standalone/ns1/persistent/test --cursorName s1 --command print-cursor
'''
def printManagedCursorCommand(zk, mlPath, cursorName):
    try:
        if cursorName:
            print getManagedCursorInfo(zk, mlPath)[cursorName]
        else:
            print 'Usage: --command {} [--cursorName]'.format(printCursorsCommands)
    except Exception as e:
        traceback.print_exc()
        print 'No cursor found for {}/{}'.format(mlPath, cursorName)

'''
delete specific ledgerIds from the managed-ledger info
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
deleteLedgerIds : str
    comma separated deleting ledger-ids (eg: 123,124)
eg:
--zkServer localhost:2181 --mlPath sample/standalone/ns1/persistent/test --command delete-ml-ledgers --ledgerIds 3
'''    
def deleteMLLedgerIdsCommand(zk, mlPath, deleteLedgerIds):
    try:
        if deleteLedgerIds:
            deletLedgerIds = set(deleteLedgerIds.split(","))
            deletLedgerIdSet = set()
            for id in deletLedgerIds:
                deletLedgerIdSet.add(long(id))
            print     deletLedgerIdSet
            deleteLedgerIdsFromManagedLedgerInfo(zk, mlPath, deletLedgerIdSet)
        else:
            print 'Usage: --command {} [--ledgerIds]'.format(deleteMlLedgerIds)
    except Exception as e:
        traceback.print_exc()
        print 'Failed to delete ml-ledger_ids {} due to {}'.format(mlPath, repr(e))

'''
Update mark-delete position of the given managed-cursor
Parameters
----------
zk : KazooClient
   Zookeeper-client instance to query zk-client.
mlPath : str
    managed-ledger path
cursorName : str
    managed-cursor path
markDeletePosition: str
    markDeletePosition combination of <ledgerId>:<entryId> (eg. 123:1)

eg:
--zkServer localhost:2181 --mlPath sample/standalone/ns1/persistent/test --cursorName s1 --cursorMarkDelete 0:1 --command update-mark-delete-cursor 
'''    
def updateMarkDeleteOfCursorCommand(zk, mlPath, cursorName, markDeletePosition):
    try:
        if cursorName:
            if markDeletePosition:
                positionPair = markDeletePosition.split(":")
                if len(positionPair) == 2:
                    updateCursorMarkDelete(zk, mlPath + "/" + cursorName, (long(positionPair[0])), long(positionPair[1]))
                else:
                    print "markDeletePosition must be in format <ledger_id>:<entry_id>"
            else:
                print 'Usage: --command {} [----cursorName] [--cursorMarkDelete]'.format(updateMakDeleteCursor)
        else:
            print 'Usage: --command {} [--cursorName] [--cursorMarkDelete]'.format(updateMakDeleteCursor)
    except Exception as e:
        traceback.print_exc()
        print 'Failed to update ml-cursor {}/{} due to {}'.format(mlPath, cursorName, repr(e))
            
if __name__ in '__main__':
        parser = argparse.ArgumentParser()
        commandHelpText = 'Managed-ledger command: \n{}, {}, {}, {}'.format(printMlCommand, deleteMlLedgerIds, printCursorsCommands, updateMakDeleteCursor)
        parser.add_argument("--zkServer", "-zk", required=True, help="ZooKeeperServer:port")
        parser.add_argument("--command", "-cmd", required=True, help=commandHelpText)
        parser.add_argument("--mlPath", "-mlp", required=True, help="Managed-ledger path")
        parser.add_argument("--ledgerIds", "-lid", required=False, help="Delete ledger ids: comma separated")
        parser.add_argument("--cursorName", "-cn", required=False, help="ML-cursor name")
        parser.add_argument("--cursorMarkDelete", "-cm", required=False, help="Cursor mark delete position: <ledger_id>:<entry_id>")
        args = parser.parse_args()

        zkSrvr = args.zkServer
        command = args.command
        mlPath = managedLedgerPath + args.mlPath
        deleteLedgerIds = args.ledgerIds
        cursorName = args.cursorName
        cursorMarkDelete = args.cursorMarkDelete

        zk = KazooClient(hosts=zkSrvr)
        zk.start()
        
        if command == printMlCommand:
            printManagedLedgerCommand(zk, mlPath)
        elif command == deleteMlLedgerIds:
            deleteMLLedgerIdsCommand(zk, mlPath, deleteLedgerIds)
        elif command == printCursorsCommands:
            printManagedCursorCommand(zk, mlPath, cursorName)
        elif command == updateMakDeleteCursor:
            updateMarkDeleteOfCursorCommand(zk, mlPath, cursorName, cursorMarkDelete)
        else:
            print '{} command not found. supported command : {}'.format(command, commandHelpText)
