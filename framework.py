#---------------------------------------------------------Import
from pyclbr import Function
from xml.sax.xmlreader import InputSource
import yaml
import csv
import os
import time
import logging
import threading
from waiting import wait
#---------------------------------------------------------
    #'%Y-%m-%d %H:%M:%S.%f'
#---------------------------------------------------------Logging
Folder_Path="Dataset\Milestone3"
Input_File_Path="Milestone3A"
logging.basicConfig(filename=Input_File_Path+".log",
                    format='%(asctime)s.%(msecs)06d;%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
#---------------------------------------------------------Thread
lock = threading.Lock()
#---------------------------------------------------------

tasks=[]
Exec_Time=0
Task_Outputs={}
#---------------------------------------------------------
def yaml_loader(path,name):
    filepath=os.path.join(os.getcwd(),path)
    with open(filepath+name,'r') as file:
        workflow=yaml.safe_load(file)
    return workflow
#---------------------------------------------------------
def csv_loader(path,name):
    DataTable=[]
    filepath=os.path.join(os.getcwd(),path)
    with open(filepath+name,'r') as data:
        for line in csv.DictReader(data):
            DataTable.append(line)
            #print(line)
    return DataTable

def text_loader(path,name):
    
    DataTable={}
    filepath=os.path.join(os.getcwd(),path)
    a_file = open(filepath+name,'r')
    for line in a_file:
        lines=line.split()
    DataTable[lines[0]]=int(lines[0])
    for i in range(1,len(lines)):
        if(lines[i]!=">>" and lines[i-1]==">>"):
            DataTable[lines[i]]=DataTable[lines[i-2]]-1
        elif(lines[i]!=">>" and lines[i-1]=="<<"):
            DataTable[lines[i]]=DataTable[lines[i-2]]+1
    #print(DataTable)

    return DataTable
#---------------------------------------------------------
def TimeFunction(Execution_Time):
    global Exec_Time
    time.sleep(Execution_Time)
    Exec_Time=Exec_Time+Execution_Time
    #print("Current Time: ",Exec_Time)
#---------------------------------------------------------
def seq_flow(prev_key,dicts):
    global tasks
    logger.info(prev_key+" Entry")
    for key,vals in dicts.items():
            new_key=prev_key+"."+key
            # logger.info(new_key+" Entry")
            if(vals['Type']=='Flow'):
                if(vals['Execution']=='Sequential'):
                    seq_flow(new_key,vals['Activities'])
                else:
                    par_flow(new_key,vals['Activities'])
            else:
                Execute_Tasks(new_key,vals,"Serial")
            #logger.info(new_key+" Exit")
    logger.info(prev_key+" Exit")
#---------------------------------------------------------   
def par_flow(prev_key,dicts):
    objects=[]
    logger.info(prev_key+" Entry")
    for key,vals in dicts.items():
            new_key=prev_key+"."+key
            if(vals['Type']=='Flow'):
                if(vals['Execution']=='Sequential'):
                    t1 = threading.Thread(target=seq_flow, args=(new_key,vals['Activities'],))
                    #seq_flow(new_key,vals['Activities'])
                else:
                    t1 = threading.Thread(target=par_flow, args=(new_key,vals['Activities'],))
                    #par_flow(new_key,vals['Activities'])
                objects.append(t1)
                #logger.info(new_key+" Exit")
            else:
                t1 = threading.Thread(target=Execute_Tasks, args=(new_key,vals,"Parallel",))
                objects.append(t1)
    
    for obj in objects:
        obj.start()
    for obj in objects:
        obj.join()
    logger.info(prev_key+" Exit")
#---------------------------------------------------------
def DataLoad(Filename):
    DataTable=csv_loader(Folder_Path,"\\"+Filename)
    return DataTable

#---------------------------------------------------------
def check_data_present(Condition):
    if(Condition[0] not in Task_Outputs):
        return False
    return True
#---------------------------------------------------------
def Binning(RuleFilename,DataSet_Name):
    #print("Binning")
    #print(RuleFilename," ",DataSet_Name)
    Bin_rule=DataLoad(RuleFilename)
    #BinRules={}
    Dataset=Task_Outputs[DataSet_Name]
    for Bins in Bin_rule:
        BinID=Bins['BIN_ID']
        BinSignal=Bins['RULE']
        BinRange=BinSignal.split()
        if(len(BinRange)==3):
            if(BinRange[1]=='<'):
                HighRange=BinRange[2]
                LowRange=0
            else:
                HighRange=BinRange[2]
                LowRange=0
        else:
            if(BinRange[1]=='>'):
                LowRange=BinRange[2]
                HighRange=BinRange[6]
            else:
                LowRange=BinRange[6]
                HighRange=BinRange[2]
        LowRange=int(LowRange)
        HighRange=int(HighRange)
        for i in range(len(Dataset)):
            if(int(Dataset[i]['Signal'])>LowRange and int(Dataset[i]['Signal'])<HighRange):
                Dataset[i]['Bincode']=BinID
        return Dataset

def MergeResults(Inputs):
    #print(Inputs)
    #print("MergeResults")
    PrecendenceFile=text_loader(Folder_Path,"\\"+Inputs["PrecedenceFile"])
    #print(PrecendenceFile)

    Initial_DataSet=Task_Outputs[Inputs["DataSet1"]]
    #print(Initial_DataSet)
    for key,val in Inputs.items():
       if(key!="PrecedenceFile" and key!="DataSet1"):
           New_DataSet=Task_Outputs[val]
           flag=0
           for i in range(len(New_DataSet)):
               for j in range(len(Initial_DataSet)):
                   if(New_DataSet[i]['Id']==Initial_DataSet[j]['Id']):
                       if( PrecendenceFile[New_DataSet[i]['Bincode']] >PrecendenceFile[Initial_DataSet[i]['Bincode']]):
                           PrecendenceFile[Initial_DataSet[i]['Bincode']]=PrecendenceFile[New_DataSet[i]['Bincode']]
    return New_DataSet

def ExportResults(Inputs):
    #print("ExportResults")
    Filename=Inputs["FileName"]
    DataSet=Task_Outputs[Inputs["DefectTable"]]
    file = open(Filename, "w",newline='')
    keys=DataSet[0].keys()
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(DataSet)
    file.close()

#---------------------------------------------------------
def Execute_Tasks(key,Activities,Exec_Type):
    logger.info(key+" Entry")
    Inputs=Activities['Inputs']
    Function_Name=Activities['Function']

    flag=0
    if("Condition" in Activities):
        Condition=Activities["Condition"]
        Condition=Condition.split()
        #print(key," ",Condition)
        if(Condition[0] not in Task_Outputs):
            present=wait(lambda: check_data_present(Condition), timeout_seconds=120, waiting_for="Task Output")
            if(present==False):
                flag=1
        else:
            if(Condition[1]=='>'):
                if(Task_Outputs[Condition[0]]<int(Condition[2])):
                    logger.info(key+" Skipped")
                    flag=1
            elif(Condition[1]=='<'):
                if(Task_Outputs[Condition[0]]>int(Condition[2])):
                    logger.info(key+" Skipped")
                    flag=1
    if(flag==0):
        if(Function_Name=="TimeFunction"):
            ET=Inputs['ExecutionTime']
            Function_Input=Inputs['FunctionInput']
            if(Function_Input in Task_Outputs):
                logger.info(key+" Executing "+Function_Name+" ("+str(Task_Outputs[str(Function_Input)])+", "+ET+" )")
            else:
                logger.info(key+" Executing "+Function_Name+" ( "+Function_Input+", "+ET+" )")
            TimeFunction(int(ET))
        elif(Function_Name=="DataLoad"):
            Filename=Inputs["Filename"]
            logger.info(key+" Executing "+Function_Name+" ("+Filename+")")
            DataTable=DataLoad(Filename)
            Task_Outputs["$("+key+".DataTable"+")"]=DataTable
            Task_Outputs["$("+key+".NoOfDefects"+")"]=len(DataTable)
        elif(Function_Name=="Binning"):
            lock.acquire()
            RuleFilename=Inputs["RuleFilename"]
            DataSet=Inputs["DataSet"]
            logger.info(key+" Executing "+Function_Name+" ("+RuleFilename+" , "+DataSet+")") 
            DataSet=Binning(RuleFilename,DataSet)
            Task_Outputs["$("+key+".BinningResultsTable"+")"]=DataSet
            lock.release()
        elif(Function_Name=="MergeResults"):
            DataSet=MergeResults(Inputs)
            Args=""
            for x in Inputs:
                Args=Args+(Inputs[x])
                Args=Args+(" , ")
            Args=Args[:len(Args)-2]
            #print(s)
            logger.info(key+" Executing "+Function_Name+" ("+Args+")") 
            Task_Outputs["$("+key+".MergedResults"+")"]=DataSet
        elif(Function_Name=="ExportResults"):
            logger.info(key+" Executing "+Function_Name+" ("+Inputs["FileName"]+" , "+Inputs["DefectTable"]+")") 
            ExportResults(Inputs)
    logger.info(key+" Exit")
#---------------------------------------------------------

def main():
    workflow=yaml_loader(Folder_Path,"\\"+Input_File_Path+".yaml")
    queue=[]
    queue.append(workflow)
    for dicts in queue:
        for key,vals in dicts.items():
            #logger.info(key+" Entry")
            if(vals['Type']=='Flow'):
                if(vals['Execution']=='Sequential'):
                    seq_flow(key,vals['Activities'])
                else:
                    par_flow(key,vals['Activities'])
            

main()