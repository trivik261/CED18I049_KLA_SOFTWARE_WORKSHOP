#---------------------------------------------------------Import
import yaml
import csv
import os
import time
import logging
import threading
#---------------------------------------------------------
    #'%Y-%m-%d %H:%M:%S.%f'
#---------------------------------------------------------Logging
Folder_Path="Dataset\Milestone2"
Input_File_Path="Milestone2A"
logging.basicConfig(filename=Input_File_Path+".log",
                    format='%(asctime)s.%(msecs)06d;%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

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
    #print(Filename)
    DataTable=csv_loader(Folder_Path,"\\"+Filename)
    return DataTable
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
            logger.info(key+" Executing "+Function_Name+" ( "+Function_Input+", "+ET+" )")
            TimeFunction(int(ET))
        elif(Function_Name=="DataLoad"):
            Filename=Inputs["Filename"]
            logger.info(key+" Executing "+Function_Name+" ("+Filename+")")
            DataTable=DataLoad(Filename)
            Task_Outputs[key+".DataTable"]=DataTable
            Task_Outputs["$("+key+".NoOfDefects"+")"]=len(DataTable)
    logger.info(key+" Exit")
#---------------------------------------------------------

def main():
    #workflow=yaml_loader("Dataset\Examples\Milestone1","\Milestone1_Example.yaml")
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
            #logger.info(key+" Exit")               
    #print(Task_Outputs["M2A_Workflow.TaskA.NoOfDefects"])
    # if("M2A_Workflow.TaskA.NoOfDefectss" in Task_Outputs):
    #     print("EYS")
    # else:
    #     print("NO")
    #print(Task_Outputs.has_key("M2A_Workflow.TaskA.NoOfDefects"))

main()