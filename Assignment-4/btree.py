import math
from disk_relations import *

# A BTreeBlock is a block that stores a single node of a B+-tree. We maintain the
# following information in that block:
#     The list with alternating keys and pointers
#        The pointers may be to other B+-Tree blocks, or to specific tuples in a RelationBlock
#     A pointer to the Parent B+-tree Block ("None" for the root)
#    Whether the node is a leaf node
#    We also compute a "maxlen", i.e., the maximum length of the above list given the key and pointer sizes
# NOTE: As with the rest of the code here, this is an oversimplification but attempts to capture only 
# the key constructs/concepts
class BTreeBlock(Block):
    def __init__(self, blockNumber, keysize, isLeaf, parent = None):
        Block.__init__(self, blockNumber)
        self.keysize = keysize
        self.parent = parent
        self.keysAndPointers = [None]
        self.isLeaf = isLeaf
        # We can compute aproximately how many pointers can be held in a block given the keysize
        # We will assume all pointers require a fixed size, which may not always be true
        # We will ignore the space taken by variables like isLeaf, parent, etc., for simplicity
        self.maxlen = int((self.size - Globals.pointerSize)/(keysize + Globals.pointerSize)) * 2 + 1

    def __str__(self):
        if self.parent is not None:
            return "Block No. {}, Type: BTree, Parent: {}: ".format(self.blockNumber, self.parent.blockNumber) + ", ".join([str(l) for l in self.keysAndPointers])
        else:
            return "Block No. {}, Type: BTree, Parent: None: ".format(self.blockNumber) + ", ".join([str(l) for l in self.keysAndPointers])

    def html_str(self):
        return "{}: ".format(self.blockNumber) + ", ".join([str(self.keysAndPointers[l])[:6] for l in range(1, len(self.keysAndPointers), 2)])

    def hasSpace(self):
        return len(self.keysAndPointers) < self.maxlen

    # this function assumes that there is space
    def addPointer(self, ptr, key):
        #print("Before inserting " + str(self))
        for index in range(1, len(self.keysAndPointers), 2):
            if key <= self.keysAndPointers[index]:
                self.keysAndPointers.insert(index-1, key)
                self.keysAndPointers.insert(index-1, ptr)
                return
        # The new key should be at the end, but before the last pointer
        self.keysAndPointers.insert(len(self.keysAndPointers)-1, ptr)
        self.keysAndPointers.insert(len(self.keysAndPointers)-1, key)
        #print( "After inserting " + str(self))

    # Recursive search procedure -- follow the pointers to the leaf, and then scan the leaf nodes using
    # the next pointers.
    def searchByRange(self, keystart, keyend, ret = None): 
        if ret is None:
            ret = [ ]
        """This is a recursive procedure that either return 0 or more pointers to the data"""
        if self.isLeaf:
            for index in range(1, len(self.keysAndPointers), 2):
                if keystart <= self.keysAndPointers[index] <= keyend:
                    ret.append(self.keysAndPointers[index-1])
                elif keyend < self.keysAndPointers[index]:
                    # We are finished searching, return
                    return ret

            # If we are here, that means we may need to follow the pointer chain 
            if self.keysAndPointers[len(self.keysAndPointers) - 2] <= keyend:
                nextPtr = self.keysAndPointers[len(self.keysAndPointers) - 1]
                if nextPtr is not None:
                    return nextPtr.getBlock().searchByRange(keystart, keyend, ret)
                else: 
                    return ret
            else: 
                return ret
        else:
            for index in range(1, len(self.keysAndPointers), 2):
                if keystart < self.keysAndPointers[index]:
                    found = True
                    return self.keysAndPointers[index-1].getBlock().searchByRange(keystart, keyend, ret)
            # Need to follow the last pointer on the page
            return self.keysAndPointers[len(self.keysAndPointers)-1].getBlock().searchByRange(keystart, keyend, ret)

    # The following roughly implements the algorithm shown in Figure 11.15
    def insert(self, key, ptr):
        if self.isLeaf:
            # If there is space, insert into here and we are done
            # If no space, insert and split into two and pass the pointer back up
            if self.hasSpace():
                self.addPointer(ptr, key)
                return None
            else: 
                # print( "Old node too full " + str(self))
                lprime = Disk.addBlock(BTreeBlock(-1, self.keysize, isLeaf = True, parent = self.parent))
                # The addPointer function doesn't check for size, so we can go ahead and insert into it
                # In a real implementation, we would have to copy the list keysAndPointers somewhere else
                # in memory to do this
                self.addPointer(ptr, key) 
                # print ("Old node too full -- added -- " + str(self))
                oldlist = self.keysAndPointers
                self.keysAndPointers = list()
                lprime.keysAndPointers = list()

                for i in range(0, int(len(oldlist)/4)*2): 
                    self.keysAndPointers.append(oldlist[i])
                self.keysAndPointers.append(Pointer(lprime.blockNumber))

                for i in range(int(len(oldlist)/4)*2, len(oldlist)): 
                    lprime.keysAndPointers.append(oldlist[i])

                return (self, lprime.keysAndPointers[1], lprime)
        else:
            # Recurse into the appropriate child
            # The function may return a new node and a key to be added -- do that in the case
            found = False
            for index in range(1, len(self.keysAndPointers), 2):
                if key < self.keysAndPointers[index]:
                    found = True
                    ret = self.keysAndPointers[index-1].getBlock().insert(key, ptr)
                    break
            if not found: 
                index = len(self.keysAndPointers)
                ret = self.keysAndPointers[len(self.keysAndPointers)-1].getBlock().insert(key, ptr)
            if ret is not None:
                if self.hasSpace():
                    self.keysAndPointers.insert(index, Pointer(ret[2].blockNumber))
                    self.keysAndPointers.insert(index, ret[1])
                else:
                    # The following logic is very similar to what we do above with leaves
                    lprime = Disk.addBlock(BTreeBlock(-1, self.keysize, isLeaf = False, parent = self.parent))

                    self.keysAndPointers.insert(index, Pointer(ret[2].blockNumber))
                    self.keysAndPointers.insert(index, ret[1])

                    oldlist = self.keysAndPointers
                    self.keysAndPointers = list()
                    lprime.keysAndPointers = list()

                    midpoint = int(len(oldlist)/4)*2
                    for i in range(0, midpoint+1): 
                        self.keysAndPointers.append(oldlist[i])
                    kdoubleprime = oldlist[midpoint+1]
                    for i in range(0, len(self.keysAndPointers), 2):
                        self.keysAndPointers[i].getBlock().parent = Pointer(self.blockNumber)


                    for i in range(midpoint+2, len(oldlist)): 
                        lprime.keysAndPointers.append(oldlist[i])
                    for i in range(0, len(lprime.keysAndPointers), 2):
                        lprime.keysAndPointers[i].getBlock().parent = Pointer(lprime.blockNumber)

                    return (self, kdoubleprime, lprime)

    def collectNodes(self, mylevel, nodesByLevel):
        if nodesByLevel[mylevel] is None:
            nodesByLevel[mylevel] = [self]
        else: 
            nodesByLevel[mylevel].append(self)
        if not self.isLeaf:
            for index in range(0, len(self.keysAndPointers), 2):
                self.keysAndPointers[index].getBlock().collectNodes(mylevel+1, nodesByLevel)

    def findSiblingWithSameParent(self, parentBlock):
        print(parentBlock)
        for index in range(0, len(parentBlock.keysAndPointers), 2):
            print("** {} - {} - {}".format(index, parentBlock.keysAndPointers[index].blockNumber, self.blockNumber))
            if parentBlock.keysAndPointers[index].blockNumber == self.blockNumber:
                if index != 0:
                    return (parentBlock.keysAndPointers[index-2].getBlock(), parentBlock.keysAndPointers[index-1], self)
                else: 
                    return (self, parentBlock.keysAndPointers[index+1], parentBlock.keysAndPointers[index+2].getBlock())
        raise ValueError("This should not happen")

    def canMergeWith(self, otherBlock): 
        if self.isLeaf:
            # For leaves, we have one less pointer to worry about if we merge
            return (len(self.keysAndPointers) + len(otherBlock.keysAndPointers) - 1) <= self.maxlen
        else:
            # For interior nodes, on the other hand, we actually need one more key to be stored if we merge
            return (len(self.keysAndPointers) + len(otherBlock.keysAndPointers) + 1) <= self.maxlen

    def mergeEntriesFromBlock(self, otherBlock, key = None):
        # Since we are going to delete otherBlock, need to re-point all the children of otherBlock to self
        if self.isLeaf:
            for i in range(0, len(otherBlock.keysAndPointers)-2, 2):
                if otherBlock.keysAndPointers[i] is not None:
                    otherBlock.keysAndPointers[i].getBlock().parent = Pointer(self.blockNumber)
            # delete the last pointer and copy over
            del self.keysAndPointers[-1]
            self.keysAndPointers.extend(otherBlock.keysAndPointers)
        else:
            for i in range(0, len(otherBlock.keysAndPointers), 2):
                if otherBlock.keysAndPointers[i] is not None:
                    otherBlock.keysAndPointers[i].getBlock().parent = Pointer(self.blockNumber)
            # Here we need to add in the key that we get from the parent of these two nodes
            self.keysAndPointers.append(key)
            self.keysAndPointers.extend(otherBlock.keysAndPointers)

    def getParentKey(self):
        parentBlock = self.parent.getBlock()
        for index in range(0, len(parentBlock.keysAndPointers), 2):
            if parentBlock.keysAndPointers[index].blockNumber == self.blockNumber:
                return parentBlock.keysAndPointers[index+1]

    def redistributeWithBlock(self, otherBlock):
        print("Redistributing entries between " + str(self) + " and " + str(otherBlock))
        #raise ValueError("Functionality to be implemented")
        kp = self.getParentKey()
        # if otherBlock is underfull, transfer "from left to right"
        if otherBlock.isUnderfull():
            if otherBlock.isLeaf:
                # transfer pointer/key pair (km, pm) from end of otherBlock to beginning of self
                pm, km = self.keysAndPointers[-3:-1]
                del self.keysAndPointers[-3:-1]   
                otherBlock.keysAndPointers.insert(0,pm)
                otherBlock.keysAndPointers.insert(1,km)
                # replace 'parent key' with km
                return km
            else:    
                # get last key and pointer from self 
                km1, pm = self.keysAndPointers[-2:]
                del self.keysAndPointers[-2:]
                # Update parent pointers of the blocks which were just removed
                pm.getBlock().parent = Pointer(otherBlock.blockNumber)
                # move km1 to the key of the parent, and move the key of the parent to otherBlock
                otherBlock.keysAndPointers.insert(0,pm)
                otherBlock.keysAndPointers.insert(1,kp)
                return km1
        # else, transfer "from right to left"
        elif self.isUnderfull():
            if self.isLeaf:
                # move first pointer & key from otherBlock to self, set parent key as new first entry in otherBlock 
                p1, k1 = otherBlock.keysAndPointers[:2]
                del otherBlock.keysAndPointers[:2]
                self.keysAndPointers.insert(-1,p1)
                self.keysAndPointers.insert(-1,k1)
                return otherBlock.keysAndPointers[1]
            else:
                # TODO FIX
                print("!!! RIGHT TO LEFT RESDISTRIBUTION, NON-LEAF !!!")
                p1, k1 = otherBlock.keysAndPointers[:2]
                print(f"p1: {p1}, k1: {k1}")
                del otherBlock.keysAndPointers[:2]
                p1.getBlock().parent = Pointer(self.blockNumber)
                self.keysAndPointers.append(kp)
                self.keysAndPointers.append(p1)
                return k1
        else:
            raise ValueError("Sibling blocks are both underfull, unable to redistribute")
        
    def isUnderfull(self):
        # Root can't be underful
        if self.parent is None:
            return False
        if self.isLeaf:
            # Max number of pointers = (maxlen+1)/2, so we should have at least half of that, i.e.,
            # at least math.ceil(maxlen+1)/4 pointers
            return (len(self.keysAndPointers)-1)/2 < math.ceil((self.maxlen+1)/4)
        else: 
            return (len(self.keysAndPointers)+1)/2 < math.ceil((self.maxlen+1)/4)

    def mergeOrRedistributeWithSibling(self):
        parentBlock = self.parent.getBlock()
        (block1, key, block2) = self.findSiblingWithSameParent(parentBlock)
        if block1.canMergeWith(block2):
            # Merge the two nodes
            block1.mergeEntriesFromBlock(block2, key)
            # Need to delete block2 pointer from the parent node
            return block2
        else: 
            # Need to redistribute pointers between the two and then modify the parent accordingly
            newKey = block1.redistributeWithBlock(block2)

            # Replace key with newKey in the parent
            for index in range(0, len(parentBlock.keysAndPointers), 2):
                if parentBlock.keysAndPointers[index].blockNumber == block1.blockNumber:
                    parentBlock.keysAndPointers[index+1] = newKey
                    # We are now done -- return None and stop
                    return None 
            raise ValueError("This should not happen")

    # The following roughly (and partially) implements the algorithm from Figure 11.19
    def delete(self, key, ptr):
        if self.isLeaf:
            # First remove that key, ptr
            found = False
            for index in range(1, len(self.keysAndPointers), 2):
                #print( "Comparing {} and {} with {} and {}".format(key, str(ptr), self.keysAndPointers[index], str(self.keysAndPointers[index-1])))
                if key == self.keysAndPointers[index] and ptr == self.keysAndPointers[index-1]:
                    found = True
                    self.keysAndPointers.pop(index-1)
                    self.keysAndPointers.pop(index-1)
                    break
            if not found:
                raise ValueError("This should not happen")

            if self.isUnderfull():
                return self.mergeOrRedistributeWithSibling()
            else:
                return None
        else: 
            # Recurse into the appropriate child
            # The function may return a ptr and a key to be deleted 
            found = False
            for index in range(1, len(self.keysAndPointers), 2):
                if key < self.keysAndPointers[index]:
                    found = True
                    ret = self.keysAndPointers[index-1].getBlock().delete(key, ptr)
                    break
            if not found: 
                index = len(self.keysAndPointers)
                ret = self.keysAndPointers[len(self.keysAndPointers)-1].getBlock().delete(key, ptr)

            if ret is None:
                # We are done -- return None and stop
                return None
            else: 
                # We need to do a deletion in this node -- ret is the block that needs to be deleted
                # This may be different from the block that we followed down, so search again
                found = False
                for index in range(0, len(self.keysAndPointers), 2):
                    if ret.blockNumber == self.keysAndPointers[index].blockNumber:
                        found = True
                        self.keysAndPointers.pop(index-1)
                        self.keysAndPointers.pop(index-1)
                        break
                if not found:
                    raise ValueError("This should not happen")

                if self.isUnderfull():
                    return self.mergeOrRedistributeWithSibling()
                else:
                    return None

    def collectNodes(self, mylevel, nodesByLevel):
        if nodesByLevel[mylevel] is None:
            nodesByLevel[mylevel] = [self]
        else: 
            nodesByLevel[mylevel].append(self)
        if not self.isLeaf:
            for index in range(0, len(self.keysAndPointers), 2):
                self.keysAndPointers[index].getBlock().collectNodes(mylevel+1, nodesByLevel)

    ## This code checks whether: 
    ##          the node is Underfull or has too many keysAndPointers
    ##          if the keys and pointers are in the right order (i.e., (key, ptr, key, ptr, ...) pattern
    ##          the keys are ordered
    ## It DOES NOT check for other issues that may make the tree invalid
    def checkStructureValidity(self):
        # check whether full
        if self.isUnderfull() or len(self.keysAndPointers) > self.maxlen:
            return "Length Constraint Violated for {}".format(self)

        # Check the order of keys
        for i in range(0, len(self.keysAndPointers)-2, 2):
            if not isinstance(self.keysAndPointers[i], Pointer): 
                return "One of the pointers doesn't look like a pointer for {}".format(self)
        if not isinstance(self.keysAndPointers[-1], Pointer) and self.keysAndPointers[-1] is not None:
                return "The last pointers doesn't look like a pointer, and is not None, for {}".format(self)

        for i in range(1, len(self.keysAndPointers), 2):
            if not isinstance(self.keysAndPointers[i], str):
                return "One of the keys doesn't look like a 'str' for {}".format(self)

        # Check the order of keys
        for i in range(3, len(self.keysAndPointers), 2):
            if self.keysAndPointers[i] < self.keysAndPointers[i-2]:
                return "Keys not ordered for {}".format(self)
        return "No simple structural issues found for {}".format(self)

class BTreeIndex:
    def __init__(self, keysize, relation, attribute):
        self.rootPointer = Pointer(Disk.addBlock(BTreeBlock(-1, keysize, isLeaf = True, parent = None)).blockNumber)
        self.relation = relation
        self.attribute = attribute
        # Initialize the index with everything in the relation
        relation.addNewIndex(self, attribute)
    def root(self):
        return self.rootPointer.getBlock()
    def insert(self, key, dataptr):
        rootBlock = self.root()
        ret = rootBlock.insert(key, dataptr)
        if ret is not None:
            # Root was split -- create a new root
            newRoot = Disk.addBlock(BTreeBlock(-1, rootBlock.keysize, isLeaf = False, parent = None))
            newRoot.keysAndPointers = [Pointer(ret[0].blockNumber), ret[1], Pointer(ret[2].blockNumber)]
            self.rootPointer = Pointer(newRoot.blockNumber)
            ret[0].parent = self.rootPointer
            ret[2].parent = self.rootPointer

    # We will only directly support a range query, and convert a single key query into a range query
    def searchByKey(self, key):
        return self.root().searchByRange(key, key, None)

    def searchByRange(self, keystart, keyend):
        return self.root().searchByRange(keystart, keyend, None)

    def delete(self, key, ptr):
        rootBlock = self.root()
        rootBlock.delete(key, ptr)
        # Need to deal with a pathological case here
        if len(rootBlock.keysAndPointers) == 1:
            self.rootPointer = rootBlock.keysAndPointers[0]
            self.rootPointer.getBlock().parent = None

    def printTree(self):
        print("================================================================================")
        print("Printing BTree Index on Relation " + self.relation.relname + " on Attribute " + self.attribute)
        nodesByLevel = [None] * 10
        self.root().collectNodes(0, nodesByLevel)

        print("--- Level 0 (root): ")
        print("    " + str(self.root()))
        for i in range(1, 10):
            if nodesByLevel[i] is not None:
                print("--- Level {}: ".format(i))
                for node in nodesByLevel[i]:
                    print("    " + str(node))

    ## This code will do some basic validity checks against the tree.
    def checkStructureValidity(self):
        print(self.root().checkStructureValidity())

        nodesByLevel = [None] * 10
        self.root().collectNodes(0, nodesByLevel)

        for i in range(1, 10):
            if nodesByLevel[i] is not None:
                for node in nodesByLevel[i]:
                    print(node.checkStructureValidity())
