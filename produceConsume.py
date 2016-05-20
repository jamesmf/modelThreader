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

import numpy as np
from threading import Thread, Event
import time
import random
from Queue import Queue

queue = Queue(10)

def getData(data):
    return [np.random.rand(data), data**2]

class ProducerThread(Thread):
    
    def __init__(self,getData=None,data=None):
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
        nums = range(5)
        global queue, numProduced, numToProduce
        while numProduced <= numToProduce:
            if not (getData is None):
                num = self.getData(self.data)
                queue.put(num)
                numProduced+=1
            else:
                return

        return


class ConsumerThread(Thread):
    def __init__(self,useData=None):
        """
        Extends Thread constructor, allowing a useData function to be defined.
        It might require logic for handling multiple cases (i.e. train vs. test)
        and should expect a single 'data' object that matches the output from
        ProducerThread.getData, which is also user-defined
           
           input:
               useData:
                   function accepting one argument (which can be a list)
                   and returning one item to put on the Queue for the 
                   ConsumerThread to operate on
               data:
                   input to getData() function, which getData() will process.
                   Might be raw data, high level data, etc
                   
        """
        self.useData = useData
        super(ConsumerThread,self).__init__()
        
        
    def run(self):
        global queue, numConsumed, numToConsume
        while numConsumed <= numToConsume:
            data = queue.get()
            queue.task_done()
            output     = self.useData(data)
            print(data,output)
            numConsumed+=1
        return 

def useData(data):
    vec = data[0]
    scal= data[1]
    return scal*vec




numProduced = 0
numConsumed = 0
numToProduce= 1
numToConsume= 1

prodThread     = ProducerThread(getData=getData,data=2)
prodThread.start()

conThread     = ConsumerThread(useData=useData)
conThread.start()


