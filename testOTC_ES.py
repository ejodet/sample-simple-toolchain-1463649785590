import datetime
import time
from elasticsearch import Elasticsearch
import logging
from datetime import date, timedelta
import mongoHelpers
import matplotlib.pyplot as plt
import numpy as np
import os

allDates= []
ES_HOST= 'https://sl-us-dal-9-portal1.dblayer.com:10357'

# MongoDB
db= mongoHelpers.connectToMongo()
# db= mongoHelpers.connectToMongo_production()
# db= mongoHelpers.connectToMongo_P00_production()

# storageService= objectStorageHelper.connect()

IMG_PATH= "/static/images/"
# IMG_PATH =  "c:/temp/data/png/"

es_activitiesColl= db["es_activities"]
es_toolChainsColl= db["es_tool_chains"]

OTC_GA= datetime.date(2016, 05, 12)

otcCreationByDate= {}
bindActivityByDate= {}
unbindActivityByDate= {}
provisionActivityByDate= {}
bindActivityForUniqueToolChainByDate= {}
unbindActivityForUniqueToolChainByDate= {}
provisionActivityForUniqueToolChainByDate= {}
bindEventForToolsByDate= {}

colors=["greenyellow", "blue", "orangered", "mediumaquamarine","dodgerblue",  "orchid", "magenta", "yellow", "green"]

def queryES(docType, startDate, endDate):
    # use port 10358 for YS1, 10357 for YP
    es = Elasticsearch(
        hosts=[ES_HOST],
        http_auth=('esdev', 'testing'), 
        verify_certs=False)
    
    reply = es.search(
        index='dlms-*', 
        doc_type= docType, 
        fields=['@timestamp','timestamp','event','toolchain_id','service_ids'],
        q='tool:toolchain AND @timestamp:[' + startDate + ' TO ' + endDate + ']',
        sort='timestamp:desc',
        size=10000
    )
    total = reply['hits']['total']
    hits = reply['hits']['hits']
    print total
    print len(hits)
    return hits

def firsts(hit):
    return dict([(key, val[0]) for key,val in hit['fields'].items()])

# map(firsts, hits)

def populateDays(): 
    diff= date.today() - OTC_GA
    numberOfDays= diff.days
    for x in range(1, numberOfDays):        
        aDay = str(date.today() - timedelta(days=x))
        allDates.append(aDay)
    return sorted(allDates)
        
def populateMongoFromES():
    es_activitiesColl.remove({}) 
    sortedDates= populateDays()
    days= len(sortedDates)
    lastDay= sortedDates[-1]
    for x in range(0, days): 
        startDate= sortedDates[x] 
        if (startDate == lastDay):
            endDate=  str(date.today())
        else:
            endDate= sortedDates[x+1]
        
        hits= queryES('tool_id', startDate, endDate)
        if (len(hits) != 0):
            allRecords= []
            for aHit in hits:
                aRecord= aHit['fields']
                allRecords.append(aRecord)
            try:
                print "inserting " + str(len(allRecords)) + " records"
                es_activitiesColl.insert(allRecords,  check_keys= False)
            except:
                return
    
    numberOfrecords=es_activitiesColl.find().count()
    logging.info("ES Activities Collection now has " + str(numberOfrecords) + " records")
 
def creationByDate():
    allOtcIds= es_activitiesColl.distinct("toolchain_id")
    for anId in allOtcIds:
        eventsForThisChainId= es_activitiesColl.find({"toolchain_id" : anId}, sort= [("@timestamp", 1)])
        for anEvent in eventsForThisChainId:
            creationDate= anEvent["@timestamp"][0][:10]
            break
        # creations
        otcProjectsForThisDate= otcCreationByDate.get(creationDate, [])
        if (anId not in otcProjectsForThisDate):
            otcProjectsForThisDate.append(anId)
        otcCreationByDate[creationDate]= otcProjectsForThisDate
        
    generateGraphicForFeatureByDate("creationsByDate")
        
def addOneBarLabel(values1, rects1):
    opacity = 0.7
    for i,rect in enumerate(rects1):
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2., height+1, '%s'% (values1[i]),
                ha='center', va='bottom', alpha=opacity)

def generateGraphicForFeatureByDate(aFeature):
    logging.info("generateGraphicForFeature for " + aFeature + " feature")  
    if (aFeature == "creationsByDate"):
        theDict= otcCreationByDate
        theTitle= "Toolchains created, by date"
    elif (aFeature == "creatorsByDate"):
        # theDict= otcCreatorsByDate
        theTitle= "Users who created toolchains, by date"
    elif (aFeature == "otcUsersByDate"):
        # theDict= otcUsersByDate
        theTitle= "Users who added or removed tools, by date"
    elif (aFeature == "errorsByDate"):
        # theDict= otcErrorsByDate
        theTitle= "Toolchain provisioning errors, by date"
    else:
        return
    
    try:  
        N = len(theDict)
        ind = np.arange(N) 
        width = 0.55 
        opacity = 0.3
        lineOpacity = 0.1
        
        graphXLabels= []
        counts= []
        allDates= sorted(theDict.keys())
        for aDate in allDates:
            counts.append(len(theDict.get(aDate)))
            graphXLabels.append(aDate)
        
        plt.subplots(figsize=(10, 8))
        
        p1 = plt.bar(ind, counts, width, color='blue', alpha=opacity, align='center')
        
        addOneBarLabel(counts, p1)
        
        plt.ylabel("Count")
        plt.title(theTitle)
        plt.xticks(ind + width/4 , graphXLabels, ha='right', rotation=45)
        # plt.xticks(ind, graphXLabels, rotation=45)
        
        plt.tick_params(top = 'off', bottom = 'off', right = 'off')
        plt.ylim(0, max(counts)*1.1)
        plt.xlim(-0.80)
        plt.grid(axis = 'y', linestyle = '-', alpha=lineOpacity)
        plt.xticks(alpha=0.7)
        plt.yticks(alpha=0.7)
        plt.subplots_adjust(bottom= 0.15)
        # plt.show()
        fileName= aFeature + ".png"
        plt.savefig(os.path.join(IMG_PATH, fileName), dpi=60, format='png', bbox_inches='tight')
        plt.close()
        logging.info("Saved graph " + fileName)
    except Exception,e:
        logging.error("Failed to generate chart for " + aFeature + " feature")
        logging.error(str(e))
        # don't fail
        pass
    
def generateGroupedGraphicForActivitiesByDate(uniqueToolChains):
    logging.info("generateGraphicForFeature for tool chain lifecycle events")  
    
    try:
        width = 0.27 
        opacity = 0.3
        lineOpacity = 0.1
        
        graphXLabels= []
        anyActivityCount= []
        bindCounts= []
        unbindCounts= []
        provisionCounts= []
        
        if (uniqueToolChains == True):
            title= "Unique toolchain lifecycle events, by date"
            fileName= "UniqueToolChainLifeCycleEvents_grouped.png"
            allDates= sorted(bindActivityForUniqueToolChainByDate.keys())  
            N = len(bindActivityForUniqueToolChainByDate)
            ind = np.arange(N) 
            for aDate in allDates:
                bindCounts.append(len(bindActivityForUniqueToolChainByDate.get(aDate, [])))
                unbindCounts.append(len(unbindActivityForUniqueToolChainByDate.get(aDate, [])))
                provisionCounts.append(len(provisionActivityForUniqueToolChainByDate.get(aDate, [])))
                anyActivityCount.append(max(bindCounts))
                anyActivityCount.append(max(unbindCounts))
                anyActivityCount.append(max(provisionCounts))
                graphXLabels.append(aDate)
        else:
            title= "Toolchain lifecycle events, by date"
            fileName= "ToolChainLifeCycleEvents_grouped.png"
            allDates= sorted(bindActivityByDate.keys())  
            N = len(bindActivityByDate)
            ind = np.arange(N) 
            for aDate in allDates:
                bindCounts.append(len(bindActivityByDate.get(aDate, [])))
                unbindCounts.append(len(unbindActivityByDate.get(aDate, [])))
                provisionCounts.append(len(provisionActivityByDate.get(aDate, [])))
                anyActivityCount.append(max(bindCounts))
                anyActivityCount.append(max(unbindCounts))
                anyActivityCount.append(max(provisionCounts))
                graphXLabels.append(aDate)
        
        plt.subplots(figsize=(10, 8))
        
        p1 = plt.bar(ind, provisionCounts, width, color='green', alpha=opacity, align='center')
        p2 = plt.bar(ind+width, bindCounts, width, color='b', alpha=opacity, align='center')
        p3 = plt.bar(ind+(width*2), unbindCounts, width, color='orange', alpha=opacity, align='center')
        
        # addThreeBarsLabel(unbindCounts, p1, provisionCounts, p2, bindCounts, p3)
        
        autolabel(plt, p1)
        autolabel(plt, p2)
        autolabel(plt, p3)
        
        plt.ylabel("Count")
        plt.title(title)
        plt.xticks(ind + (3*width)/2 , graphXLabels, ha='right', rotation=45)
        # plt.xticks(ind, graphXLabels, rotation=45)
        
        plt.tick_params(top = 'off', bottom = 'off', right = 'off')
        plt.ylim(0, max(anyActivityCount)*1.1)
        plt.xlim(-0.80)
        plt.grid(axis = 'y', linestyle = '-', alpha=lineOpacity)
        plt.xticks(alpha=0.7)
        plt.yticks(alpha=0.7)
        plt.subplots_adjust(bottom= 0.15)
        plt.legend( (p1[0], p2[0], p3[0]), ('provision', 'bind', 'unbind') , loc="best", handlelength=1, fontsize=12)
        # plt.show()
        plt.savefig(os.path.join(IMG_PATH, fileName), dpi=60, format='png', bbox_inches='tight')
        plt.close()
        logging.info("Saved graph " + fileName)
    except Exception,e:
        logging.error("Failed to generate chart for tool chain lifecycle events")
        logging.error(str(e))
        # don't fail
        pass
   
def generateGroupedGraphicForBindEventsByDate():
    logging.info("generateGraphicForFeature for tool chain bind events")  
    
    try:  
        N = len(bindEventForToolsByDate)
        ind = np.arange(N) 
        width = 0.12
        opacity = 0.5
        lineOpacity = 0.1
        
        toolDictsByName= {}
        for aDate in bindEventForToolsByDate:
            toolsForThisDate= bindEventForToolsByDate.get(aDate).keys()
            for aTool in toolsForThisDate:
                if (toolDictsByName.has_key(aTool)):
                    continue
                toolDictsByName[aTool]= []
           
        graphXLabels= []
        allToolNames= toolDictsByName.keys()
        allValues= []
        allDates= sorted(bindEventForToolsByDate.keys())
        for aDate in allDates:
            graphXLabels.append(aDate)
            toolsWithCountForThisDate= bindEventForToolsByDate.get(aDate)
            for aToolName in allToolNames:
                theDict= toolDictsByName.get(aToolName)
                aCount= toolsWithCountForThisDate.get(aToolName, 0)
                theDict.append(aCount)
                toolDictsByName[aToolName]= theDict
                allValues.append(aCount)
             
        """        
            bindCounts.append(len(bindActivityByDate.get(aDate, [])))
            unbindCounts.append(len(unbindActivityByDate.get(aDate, [])))
            provisionCounts.append(len(provisionActivityByDate.get(aDate, [])))
            anyActivityCount.append(max(bindCounts))
            anyActivityCount.append(max(unbindCounts))
            anyActivityCount.append(max(provisionCounts))
            graphXLabels.append(aDate)
       """ 
        plt.subplots(figsize=(20, 10))
        
        plots= []
        labels= []
        idx= 0
        for aToolName, aCount in toolDictsByName.iteritems():
            aColor= colors[idx]
            p= plt.bar(ind + (idx * width), aCount, width, color=aColor, alpha=opacity, align='center')
            autolabel(plt, p)
            plots.append(p[0])
            labels.append(aToolName)
            idx +=1
            
        plt.ylabel("Count")
        plt.title("Tools bind events, by date")
        
        numberOfTools= len(toolDictsByName.keys())
        plt.xticks(ind + (numberOfTools*width)/2 , graphXLabels, ha='right', rotation=45)
        
        plt.tick_params(top = 'off', bottom = 'off', right = 'off')
        plt.ylim(0, max(allValues)*1.1)
        plt.xlim(-0.3)
        plt.grid(axis = 'y', linestyle = '-', alpha=lineOpacity)
        plt.xticks(alpha=0.7)
        plt.yticks(alpha=0.7)
        plt.subplots_adjust(bottom= 0.15)
        plt.legend(plots, labels , loc="best", handlelength=1, fontsize=12)
        # plt.show()
        fileName= "toolsBindEvents_grouped.png"
        plt.savefig(fileName, dpi=60, format='png', bbox_inches='tight')
        plt.close()
        logging.info("Saved graph " + fileName)
    except Exception,e:
        logging.error("Failed to generate chart for tool chain bind events")  
        logging.error(str(e))
        # don't fail
        pass
   
def generateStackedGraphicForActivitiesByDate():
    logging.info("generateGraphicForFeature for tool chain lifecycle events")  
    
    try:  
        N = len(bindActivityByDate)
        ind = np.arange(N) 
        width = 0.55 
        opacity = 0.3
        lineOpacity = 0.1
        
        graphXLabels= []
        anyActivityCount= []
        bindCounts= []
        unbindCounts= []
        provisionCounts= []
        allDates= sorted(bindActivityByDate.keys())
        for aDate in allDates:
            bindCounts.append(len(bindActivityByDate.get(aDate, [])))
            unbindCounts.append(len(unbindActivityByDate.get(aDate, [])))
            provisionCounts.append(len(provisionActivityByDate.get(aDate, [])))
            anyActivity= len(bindActivityByDate.get(aDate, [])) + len(unbindActivityByDate.get(aDate, [])) + len(provisionActivityByDate.get(aDate, []))
            anyActivityCount.append(anyActivity)
            graphXLabels.append(aDate)
        
        plt.subplots(figsize=(10, 8))
        
        a= np.array(unbindCounts)
        b= np.array(provisionCounts)
        p1 = plt.bar(ind, unbindCounts, width, color='orange', alpha=opacity, align='center')
        p2 = plt.bar(ind, provisionCounts, width, color='green', alpha=opacity, align='center', bottom=sum([a]))
        p3 = plt.bar(ind, bindCounts, width, color='b', alpha=opacity, align='center', bottom=sum([a, b]))
        
        addThreeBarsLabel(unbindCounts, p1, provisionCounts, p2, bindCounts, p3)
        
        plt.ylabel("Count")
        plt.title("Toolchain lifecycle events, by date")
        plt.xticks(ind + width/4 , graphXLabels, ha='right', rotation=45)
        # plt.xticks(ind, graphXLabels, rotation=45)
        
        plt.tick_params(top = 'off', bottom = 'off', right = 'off')
        plt.ylim(0, max(anyActivityCount)*1.1)
        plt.xlim(-0.80)
        plt.grid(axis = 'y', linestyle = '-', alpha=lineOpacity)
        plt.xticks(alpha=0.7)
        plt.yticks(alpha=0.7)
        plt.subplots_adjust(bottom= 0.15)
        plt.legend( (p1[0], p2[0], p3[0]), ('unbind', 'provision', 'bind') , loc="best", handlelength=1, fontsize=12)
        # plt.show()
        fileName= "toolChainLifeCycleEvents_stacked.png"
        plt.savefig(os.path.join(IMG_PATH, fileName), dpi=60, format='png', bbox_inches='tight')
        plt.close()
        logging.info("Saved graph " + fileName)
    except Exception,e:
        logging.error("Failed to generate chart for tool chain lifecycle events")
        logging.error(str(e))
        # don't fail
        pass
    
def addThreeBarsLabel(values1, rects1, values2, rects2, values3, rects3):
    opacity = 0.4
    
    for i,rect in enumerate(rects1):
        # lower bar
        height = rect.get_height()
        if (values1[i] >15):
            plt.text(rect.get_x()+rect.get_width()/2., (height/2) - 10 , '%s'% (values1[i]),
                    ha='center', va='bottom', alpha=opacity)
        # middle bar
        height= rect.get_height() + (rects2[i].get_height()/2)
        if (values2[i] >15):
            plt.text(rect.get_x()+rect.get_width()/2., height - 10, '%s'% (values2[i]),
                    ha='center', va='bottom', alpha=opacity)
        # upper bar
        height= rect.get_height() + rects2[i].get_height() + (rects3[i].get_height()/2)
        if (values3[i] >15):
            plt.text(rect.get_x()+rect.get_width()/2., height - 10, '%s'% (values3[i]),
                    ha='center', va='bottom', alpha=opacity)
        # total
        height= (rect.get_height() + rects2[i].get_height() + rects3[i].get_height())*1.01
        total= values1[i] + values2[i] + values3[i]
        plt.text(rect.get_x()+rect.get_width()/2., height, '%s'% (total),
                ha='center', va='bottom')

def autolabel(ax, rects):
    for rect in rects:
        h = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., h*1.01, '%d'%int(h),
                ha='center', va='bottom', alpha=0.7)
    
def activitiesByDate():
    allActivities= es_activitiesColl.find({})
    for anActivity in allActivities:
        # tool chains
        publishDate= anActivity["@timestamp"][0][:10]
        timeStamp= anActivity["timestamp"][0]
        activity= anActivity["event"][0]
        if (activity == "bind"):
            theDict= bindActivityByDate
        elif (activity == "unbind"):
            theDict= unbindActivityByDate
        elif (activity == "provision"):
            theDict= provisionActivityByDate
        else:
            print activity + " not taken into account"
            continue
        eventsList= theDict.get(publishDate, [])
        eventsList.append(timeStamp)
        theDict[publishDate]= eventsList
    
    generateStackedGraphicForActivitiesByDate()    
    generateGroupedGraphicForActivitiesByDate(False)
    
def activitiesForUniqueToolsChainsByDate():
    allActivities= es_activitiesColl.find({})
    for anActivity in allActivities:
        # tool chains
        publishDate= anActivity["@timestamp"][0][:10]
        aToolChainId= anActivity["toolchain_id"][0]
        activity= anActivity["event"][0]
        if (activity == "bind"):
            theDict= bindActivityForUniqueToolChainByDate
        elif (activity == "unbind"):
            theDict= unbindActivityForUniqueToolChainByDate
        elif (activity == "provision"):
            theDict= provisionActivityForUniqueToolChainByDate
        else:
            print activity + " not taken into account"
            continue
        toolChainsForThisDate= theDict.get(publishDate, [])
        if (aToolChainId not in toolChainsForThisDate):
            toolChainsForThisDate.append(aToolChainId)
        theDict[publishDate]= toolChainsForThisDate
    
    generateGroupedGraphicForActivitiesByDate(True)
    
def bindEventsForTools():
    allActivities= es_activitiesColl.find({})
    for anActivity in allActivities:
        activity= anActivity["event"][0]
        if (activity == "bind"):
            publishDate= anActivity["@timestamp"][0][:10]
            aToolId= anActivity["service_ids"][0]
            toolsWithCountForThisDate= bindEventForToolsByDate.get(publishDate, {})
            oldCountForThisTool= toolsWithCountForThisDate.get(aToolId, 0)
            newCountForThisTool= oldCountForThisTool +1
            toolsWithCountForThisDate[aToolId]= newCountForThisTool
            bindEventForToolsByDate[publishDate]= toolsWithCountForThisDate
    
    generateGroupedGraphicForBindEventsByDate()

"""    
def saveToStorageService(containerName, fileName):
    # Create a new container
    storageService.put_container(containerName)
    print "nContainer %s created successfully." % containerName
    
    # Create a file for uploading
    with open(fileName, 'w') as example_file:
        storageService.put_object(containerName, fileName, contents= "", content_type='text/plain')
    print "nFile %s saved successfully." % fileName
"""       
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
    jobStartTime= time.time()
    logging.info("Starting OTC ES v2 data analysis")
    try:
        # populateMongoFromES()
        bindEventsForTools()
        activitiesForUniqueToolsChainsByDate()
        activitiesByDate()
        creationByDate()
        
    except Exception,e:
        logging.info(str(e))
        #mailNotifier.notifyFailure("testOTC", e)
        raise
    
    jobElapsed= time.time() - jobStartTime
    logging.info("OTC data collection completed in " + str(jobElapsed) + " ms")
    

if __name__ == "__main__":
    main()