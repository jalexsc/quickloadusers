import datetime
import warnings
import json
import uuid
import os
import os.path
import requests
import io
import math
import csv
import time
import random
import logging
import dataframe_class as pd
import validator
import ast
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import backup_restore as br
import main_functions as mf
import time
from datetime import datetime
import yaml
import shutil
import main_functions as AcqErm
import notes_class as notes

################################
##USERS FUNCTION
################################
class users():
    def __init__(self,client,path_dir):
        try:    
            self.customerName=client
            self.path_dir=path_dir
            self.path_results=f"{path_dir}\\results"
            self.path_data=f"{path_dir}\\data"
            self.path_logs=f"{path_dir}\\logs"
            self.path_refdata=f"{path_dir}\\refdata"
            #self.userbyline=open(f"{self.path_logs}\\{self.customerName}_usersbyline.json", 'w') 
        except Exception as ee:
            print(f"ERROR: {ee}")
            
    def readMappingfile(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"\\userMapping.xlsx"
        print("INFO Reading mapping file")
        self.groups=self.customerName.importDataFrame(filetoload,sheetName="groups")
        #print(self.groups)
        self.departments=self.customerName.importDataFrame(filetoload,sheetName="departments")
        #self.userStatus=self.customerName.importDataFrame(filetoload,sheetName="userStatus")
        self.addressType=self.customerName.importDataFrame(filetoload,sheetName="addressType")
        #CUSTOM-FIELDS
        self.programa=self.customerName.importDataFrame(filetoload,sheetName="programa",dfname="programa")
        self.nivel=self.customerName.importDataFrame(filetoload,sheetName="nivel",dfname="nivel")
        self.modalidad=self.customerName.importDataFrame(filetoload,sheetName="modalidad",dfname="modalidad")
        self.areasAcademicas=self.customerName.importDataFrame(filetoload,sheetName="areasAcademicas",dfname="area Academica")
        with open(self.path_refdata+"\\users_mapping.json") as json_mappingfile:
            self.mappingdata = json.load(json_mappingfile)
        

        

                
    def readusers(self,client, **kwargs):
        self.readMappingfile()
        if 'dfnotes' in kwargs:
            self.dfnotes=kwargs['dfnotes']
            #print(dfnotes)
            self.customerName=notes.notes(client,self.path_dir,dataframe=self.dfnotes)
            self.swnotes=True
        else:
            self.swnotes=False
            
        orderList=[]
        if 'dfusers' in kwargs:      
            users=  kwargs['dfusers']
            
        #print(self.users)
        barcodelist=[]
        allusers=[]
        count=1
        usuarios={}
        goodusers=0
        baduser=0
        dt = datetime.now()
        dt=dt.strftime('%Y%m%d-%H-%M')
        for i, row in users.iterrows():
            dta = datetime.now()
            dta=dta.strftime('%H:%M')
            try:
                start_time = time.perf_counter()
                enduser={}
                worseusers={}
                firstName=""
                userName=""
                lastName="Error No LastName"
                email=""
                preferredContactTypeId="002"
                patronGroup=""
                departmentUsers=[]
                department=""
                printusers=True
                patronGroupId="000000-000000-000000-00000-0000000"
                if 'patronGroup' in users.columns:
                    if row['patronGroup']:
                        result=str(row['patronGroup']).strip()
                        #result=f"0{result}"
                        patronusertemp=self.searchdata_dataframe(self.groups,"LEGACY SYSTEM","FOLIO",result)
                        if patronusertemp is not None:
                            patronuserId=mf.readJsonfile(self.path_refdata,client+"_usergroups.json","usergroups",patronusertemp,"group")
                        if patronuserId is None:
                            mf.write_file(ruta=self.path_logs+"\\patronusersNotFounds.log",contenido=f"{result}")
                            printusers=False
                        else:
                            patronGroupId=str(patronuserId[0])
                        
                    enduser['type']="Patron"
                    if patronGroupId=="000000-000000-000000-00000-0000000":
                        printusers=False
                        patronGroupId=result
                    enduser['patronGroup']=patronGroupId
                    enduser['proxyFor']=[]
                    field="id"
                    id_user=""
                    if field in users.columns:
                        if row[field]: id_user=str(row[field]).strip()
                    else:    id_user=str(uuid.uuid4())
                    enduser['id']=id_user
                    linkId=id_user
                    field="requestPreference.defaultServicePointId"
                    servicePointsName=""
                    if field in users.columns:
                        if row[field]:
                            servicePointsName=str(row[field]).strip()
                            self.servicepointUser(client,id_user,servicePointsName)
                            
                    userBarcode=""
                    if 'barcode' in users.columns:
                        if row['barcode']:
                            checkbarcode=str(row['barcode']).strip()
                            countlist = barcodelist.count(str(checkbarcode))
                            if countlist>0:
                                printusers=False
                            else:
                                userBarcode=checkbarcode                        
                                barcodelist.append(userBarcode)
                                enduser['barcode']=userBarcode

                    userName=""    
                    if 'username' in users.columns:
                        if row['username']:
                            userName=str(row['username']).strip()
                        else:
                            userName=userBarcode
                            
                    enduser['username']=userName
                    externalSystemId=""
                    if 'externalSystemId' in users.columns:
                        if row['externalSystemId']:
                            externalSystemId=str(row['externalSystemId']).strip()
                    enduser['externalSystemId']=externalSystemId
                    activeUser=True
                    if 'active' in users.columns:
                        if row['active']:
                            result=self.mapping(self.userStatus,str(row['active']).strip())
                            if result is not None:
                                activeUser=result
                                
                    enduser['active']=activeUser
                    departmentUsers=[]
                    if 'departments' in users.columns:
                        if row['departments']:
                            department=str(row['departments']).strip()
                            departmentId=mf.readJsonfileRetor(self.path_refdata,client+"_departments.json","departments",department,"name")
                            #C:\Users\asoto\Documents\EBSCO\Migrations\folio\client_data\uai\refdata
                            if departmentId is not None:
                                departmentUsers.append(departmentId)
                            else:
                                mf.write_file(ruta=self.path_logs+"\\departmentNotFounds.log",contenido=f"{department}")
                    enduser['departments']=departmentUsers
                    per={}
                    if 'personal.lastName' in users.columns:
                        lastName=""
                        if row['personal.lastName']:
                            print(row['personal.lastName'])
                            lastName=str(row['personal.lastName']).strip()
                            ln=[]
                            if client=="Petrobras":
                                ln=lastName.split(' ')
                                x=len(ln)
                                lastName=ln[x-1]

                                    
                            print(f"{dta} INFO Processing user record # {count} User-LastName: {lastName}")
                            per['lastName']=lastName
                            firstName=""
                            if 'personal.firstName' in users.columns:
                                if row['personal.firstName']:
                                    firstName=str(row['personal.firstName']).strip()
                                    if client=="Petrobras":
                                        fn=firstName.split(' ')
                                        x=len(fn)
                                        firstName=fn[0]
                                per['firstName']=firstName
                            if 'personal.middleName' in users.columns:
                                if row['personal.firstName']:
                                    firstName=str(row['personal.firstName']).strip()
                                    if client=="Petrobras":
                                        ln=firstName.split(' ')
                                        x=len(ln)
                                        midName=ln[1] +" "+ln[2]
                                    
                                    if ln[x-2]=="DE" or ln[x-2]=="DO" or ln[x-2]=="DOS" or ln[x-2]=="DAS" or ln[x-2]=="DA":
                                        midName=midName+ " " +ln[x-2]
                                    else:
                                        midName=ln[1]

                                        
                                    per['middleName']=midName
                            
                            email=""
                            if 'personal.email' in users.columns:
                                if row['personal.email']:
                                    email=str(row['personal.email']).strip()
                                else:
                                    email="biblioteca@petrobras.com.br"
                                per['email']=email
                            phone=""
                            field='personal.phone'
                            if  field in users.columns:
                                if row[field]:
                                    phone=str(row[field]).strip()
                                    per['phone']=phone
                            field=f"personal.mobilePhone"
                            if field in users.columns:
                                if row[field]:
                                    mobilePhone=str(row[field]).strip()
                                    per['mobilePhone']=mobilePhone
                            personalpreferredFirstName=""
                            if 'personal.preferredFirstName' in users.columns:
                                if row['personal.preferredFirstName']:
                                    personalpreferredFirstName=row['personal.preferredFirstName']
                                    per['personal.preferredFirstName']=personalpreferredFirstName
                            addressesarray=[]
                            addressTypeId=""
                            addressLine1=""
                            addressLine2=""
                            addresses1=""
                            iter=0
                            addressTypeId="be3a54a2-9ad3-4349-ae4f-6170fa3f0ff4"        
                            sw=True
                            while sw:
                                addr={}
                                primaryAddress=False
                                ptype=""
                                field=f"personal.addresses[{iter}].addressTypeId"
                                if field in users.columns:
                                    if row[field]:
                                        addressTypeId=row[field]
                                field=f"personal.addresses[{iter}].addressLine1"
                                if field in users.columns:
                                    addr['addressTypeId']=addressTypeId
                                    addressLine1=str(row[field]).strip()
                                    addr['addressLine1']=addressLine1
                                    if iter==0:
                                        primaryAddress=True
                                    addr['primaryAddress']=primaryAddress
                                    field=f"personal.addresses[{iter}].addressLine2"
                                    if field in users.columns:
                                        if row[field]:
                                            addressLine2=str(row[field]).strip()
                                            addr['addressLine2']=addressLine2
                                    field=f"personal.addresses[{iter}].city"
                                    if field in users.columns:
                                        if row[field]:
                                            city=str(row[field]).strip()                                    
                                            addr['city']=city
                                    field=f"personal.addresses[{iter}].countryId"
                                    country=""
                                    if field in users.columns:
                                        if row[field]:
                                            country=str(row[field]).strip()                                    
                                            addr['countryId']=country
                                    else:
                                        addr['countryId']="CO"
                                        
                                    field=f"personal.addresses[{iter}].postalCode"        
                                    if field in users.columns:
                                        if row[field]:
                                            postalCode=str(row[field]).strip()                                    
                                            addr['postalcode']=postalCode
                                    field=f"personal.addresses[{iter}].region"        
                                    if field in users.columns:
                                        if row[field]:
                                            region=str(row[field]).strip()                                    
                                            addr['region']=region
                                    addressesarray.append(addr)                                    
                                else:
                                    sw=False
                                iter+=1          
                            per['addresses']= addressesarray
                            dateOfBirth=""
                            fecha_dt=""
                            field=f"personal.dateOfBirth"   
                                 
                            if field in users.columns:
                                if row[field]:
                                    try:
                                        fecha_dt=row[field]
                                        dateOfBirth=fecha_dt.strftime("%Y-%m-%d-%H:%M:%S.000+00:00")
                                        
                                    except Exception as ee:
                                        dateorder=str(row[field])
                                        dateOfBirth=date_stamp(dateorder)
                                        #dateorder=str(dateorder)
                                        #M=dateorder[0:2]
                                        #D=dateorder[3:5] 
                                        #Y=dateorder[6:10]
                                    per['dateOfBirth']=dateOfBirth
                                    
                            personalemail=""        
                            ##customFields
                            per['preferredContactTypeId']="002"
                            enduser['personal']=per
                            enrollmentDate=""
                            if 'enrollmentDate' in users.columns:
                                if row['enrollmentDate']:
                                    dateenrollment=row['enrollmentDate']
                                    enrollmentDate=dateenrollment.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
                                    enduser['enrollmentDate']=enrollmentDate
                            expirationDate=""
                            customFields=[]
                            customFieldslist=self.customFields()
                            if len(customFieldslist)>0:
                                cf={}
                                for field in customFieldslist:
                                    if field in users.columns:
                                        if row[field]:
                                            infocustom=str(row[field]).strip()
                                            toSearch=infocustom.replace(".0","")
                                            if field=="programa":
                                                if row[field]:
                                                   result=self.mapping(self.programa,toSearch)
                                                   infocustom=result
                                            elif field=="nivel":
                                                if row[field]:
                                                   result=self.mapping(self.nivel,toSearch)
                                                   infocustom=result
                                            elif field=="modalidad":
                                                if row[field]:
                                                   result=self.mapping(self.modalidad,toSearch)
                                                   infocustom=result
                                            elif field=="areasAcademicas":
                                                if row[field]:
                                                   result=self.mapping(self.areasAcademicas,toSearch)
                                                   infocustom=result
                                            elif field=="idAlternativoUsuario":
                                                if row[field]:
                                                    infocustom=infocustom
                                            if infocustom is not None:
                                                cf[field]=infocustom
                                        infocustom=""
                                enduser['customFields']=cf
                            
                            fecha_dt=""
                            if 'expirationDate' in users.columns:
                                if row['expirationDate']:
                                    try:
                                        expirationDate=str(row['expirationDate'])
                                        fecha_dt=expirationDate.strftime("%Y-%m-%dT00:00:00.000+00:00")
                                    except Exception as ee:
                                        dateorder=str(row['expirationDate'])
                                        fecha_dt=date_stamp(dateorder)
                                        #dateorder=str(dateorder)
                                        #M=dateorder[0:2]
                                        #D=dateorder[3:5] 
                                        #Y=dateorder[6:10]
                                    
                                                       #2021-12-31T05:00:00.000+00:00
                                    #print(fecha_dt)
                
                                enduser['expirationDate']=fecha_dt
                        enduser['type']= "object"
                        worseusers=enduser
                        if printusers:
                            mf.printObject(enduser,self.path_results,count,client+f"_usersbyline_{dt}",False)
                            allusers.append(enduser)
                            goodusers+=1
                            if self.swnotes:      #dataframe,toSearch,linkId):
                                self.customerName.readnotes(client,dataframe=self.dfnotes,toSearch=userBarcode,linkId=linkId)
                        else:
                            mf.printObject(worseusers,self.path_results,count,client+f"worse_usersbyline_{dt}",False)
                            baduser+=1
                        mapid={}
                        mapid['folio_id']=enduser['id']
                        mapid['legacy_id']=enduser['username']
                        mf.printObject(mapid,self.path_results,count,client+f"_mappingId_{dt}",False)
                        printusers=True                        
                        enduser={}
                        count+=1
            except Exception as ee:
                print(f"ERROR: {ee}")
        usuarios['users']=allusers
        mf.printObject(usuarios,self.path_results,count,client+f"_users-{dt}",True)
        print(f"============REPORT======================")
        print(f"RESULTS Record processed {count}")
        print(f"RESULTS Record processed {goodusers}")
        print(f"RESULTS bad records processed {baduser}")
        print(f"RESULTS end")
        
        
    def mapping(self,dftoSearch,toSearch):
        try:                    
            dataToreturn=""
            temp = dftoSearch[dftoSearch['LEGACY SYSTEM']== toSearch]
            #print("poLines founds records: ",len(temp))
            if len(temp)>0:
                for x, cptemp in temp.iterrows():
                    dataToreturn=cptemp['FOLIO']
            else:
                mf.write_file(ruta=self.path_logs+"\\workflowNotfound.log",contenido=f"{toSearch}")
                dataToreturn=None
            return dataToreturn
        
        except Exception as ee:
            print(f"ERROR: {ee}")
    
    def customFields(self):
        customlist=[]
        for i in self.mappingdata['data']:
            try:
                if i['value'] == "customFields":
                    customlist.append(i['folio_field'])
            except Exception as ee:
                print(f"ERROR: {ee}")
        return customlist
    
    
    def servicepointUser(self,client,userId,servicePointsName):
        try:
            servicePointsIds=[]
            result=""
            result=mf.readJsonfileRetor(self.path_refdata,client+"_servicepoints.json","servicepoints",servicePointsName,"name")
            if result:
                servicePointsIds.append(str(result)) #["82cb6fa0-f70b-4676-8b8f-95ef9d0d28e3","eba14df5-0a84-4348-89dd-a370c2611289"],
                defaultServicePointId=result
                spu={
                    "id": str(uuid.uuid4()),
                    "userId": userId,
                    "servicePointsIds": servicePointsIds,# 
                    "defaultServicePointId" : defaultServicePointId,
                    }
                cc=0
                mf.printObject(spu,self.path_results,cc,client+"_servicePointsUsers",False)
            else:
                mf.write_file(ruta=self.path_logs+"\\servicePointNotfound.log",contenido=f"{servicePointsName}")
        except Exception as ee:
            print(f"ERROR Service Point Users: {ee}")
            
    def searchdata_dataframe(self,dftosearchtemp,fieldtosearch,fieldtoreturn,toSearch):
        try:
            dataToreturn=None
            #print(dftosearchtemp)
            tempo = dftosearchtemp[dftosearchtemp[fieldtosearch]== toSearch]
            #fieldtoreturn=fieldtoreturn
            #print("Mapping found: ",len(temp))
            if len(tempo)>0:
                for x, cptemp in tempo.iterrows():
                    dataToreturn=str(cptemp[fieldtoreturn]).strip()
            else:
                dataToreturn=None
                if fieldtosearch=="legacy_id_sierra":
                    toSearch=f".{toSearch}"
                    tempo = dftosearchtemp[dftosearchtemp[fieldtosearch]== toSearch]
                    if len(tempo)>0:
                        for x, cptemp in tempo.iterrows():
                            dataToreturn=str(cptemp[fieldtoreturn]).strip()
            return dataToreturn
        except Exception as ee:
            print(f"ERROR: mapping {ee} {dataToreturn}")          
            
def date_stamp(ilsdate):
        dt=""
        if (ilsdate.find("/")>=0):
            dt=ilsdate
            dia=dt[0:2]
            mes=dt[3:5]
            ano=dt[6:10]
            dt=ano+"-"+mes+"-"+dia+"T"+"00:00:00+0000"
        elif (ilsdate.find(".")>=0):
            dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(ilsdate) - 2)
            hour, minute, second = self.floatHourToTime(ilsdate % 1)
            dt = str(dt.replace(hour=hour, minute=minute,second=second))+".000+0000" #Approbal by
            #2019-12-12T10:11:16.449+0000
            dt=dt.replace(" ","T")
            renewalDate=dt
        elif (ilsdate.find("-")>=0):
            dt=ilsdate
            dia=dt[8:10]
            mes=dt[5:7]
            ano=dt[0:4]
            dt=ano+"-"+mes+"-"+dia+"T"+"00:00:00+0000"
        elif (ilsdate=="0"):
            dt=""
        else:
            dt=ilsdate
            dia=dt[6:9]
            mes=dt[4:6]
            ano=dt[0:4]
            dt=ano+"-"+mes+"-"+dia+"T"+"00:00:00+0000"
        return dt
