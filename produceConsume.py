# -*- coding: utf-8 -*-
"""
Created on Thu May 19 12:25:32 2016

@author: frickjm

Abstraction for parallelizing the training/testing of a model as a 
producer/consumer system. This is useful for if preprocessing is intensive
and/or the model training is done on the GPU. It is similarly useful if the
pipeline is using data augmentation (and new examples are drawn every pass
over the dataset

ProducerThread handles the processing of data from 
a raw data source (or many). ConsumerThread accepts the output of data and 
acts on it.

Processing of the raw data is done in an arbitrary function written by the user
and passed to the constructor as the getData argument. The function should 
accept one argument containing all the raw data

Consumption of the processed data is done the same, with a single useData
function. It should also accept a single argument, which is the output of
getData()

The pipeline is controlled bya  Queue object, with a maximum queue size of 
queueSize
"""


from threading import Thread, Event
import threading
import time
import random
from Queue import Queue



class ProducerThread(Thread):
    
    def __init__(self,getData,data):
        """
        extends Thread constructor, allowing data and a data preprocessing
           function to be defined
           
           input:
               getData:
                   function accepting one argument (which can be a list)
                   and returning one item to put on the Queue for the 
                   ConsumerThread to operate on
               data:
                   input to getData() function, which getData() will process.
                   Might be raw data, high level data, etc
                   
        """
        self.getData = getData
        self.data = data
        super(ProducerThread,self).__init__()
        
        
    def run(self):
        """
        defines the operation of the ProducerThread upon .start()
        
        ProducerThread will continue to run until the global numProduced 
        exceeds the global numToProduce
        
        In each loop, the thread calls getData(self.data) and puts the result
        on the queue if there is room, then increments numProduced
        """
        global queue, numProduced, numToProduce
        
        while numProduced <= numToProduce:
            num = self.getData(self.data)
            queue.put(num)
            numProduced+=1
        return


class ConsumerThread(Thread):
    def __init__(self,useData):
        """
        Extends Thread constructor, allowing a useData function to be defined.
        It might require logic for handling multiple cases (i.e. train vs. test)
        and should expect a single 'data' object that matches the output from
        ProducerThread.getData, which is also user-defined
           
           input:
               useData:
                   function accepting one argument (which can be a list)
                   and operating on it in the desired manner (training a model,
                   for example)
        """
        self.useData = useData
        super(ConsumerThread,self).__init__()

       
        
    def run(self):
        """
        defines the operation of the ComsumerThread upon .start()
        
        ComsumerThread will continue to run until the global numConsumed 
        exceeds the global numToConsume
        
        In each loop, the thread calls queue.get() to get data then calls
        useData(data) and appends the result to consumerOutputs. It
        then increments numConsumed.
        """
        global queue, numConsumed, numToConsume, consumerOutput
        
        while numConsumed <= numToConsume:
            data = queue.get()
            queue.task_done()
            output     = self.useData(data)
            if not (output is None):
                consumerOutput.append(output)
            numConsumed+=1
        return 

def getQueue(n):
    return Queue(n)


def getData(data):
    """
    example 'getData' function'
    
    input:
        data:
            should be all the raw data in one object
            
    output:
        out:
            should be all the processed data in one object
    """
    return [data*2, data**2]

def useData(data):
    """
    example 'useData' function'
    
    input:
        data:
            should be all the processed data in one object (output by getData)
            
    output:
        out:
            Can by anything, user-defined. Will get appended to global list
            'consumerOutput' if not (output is None)
    """
    s1 = data[0]
    s2 = data[1]
    return s1*s2



queueSize = 10
numProduced = 0
numConsumed = 0
numToProduce= 100000
numToConsume= numToProduce

consumerOutput = []

queue = getQueue(queueSize)

prodThread     = ProducerThread(getData,4)
prodThread.start()

conThread     = ConsumerThread(useData)
conThread.start()

print("Threads still running:")
while len(threading.enumerate()) > 1:    
    print("\t"+str(threading.enumerate()[1:]))
    time.sleep(0.5)
print(consumerOutput[0])
