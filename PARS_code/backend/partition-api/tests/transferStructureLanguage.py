import json
PREFIX_DICT={
    "dc":"@prefix dc:<http://purl.org/dc/term>\n@prefix dc:<http://purl.org/dc/elements/1.1/>",
    "prov":"@prefix prov:<http://www.w3.org/ns/prov#>",
    "supp":"@prefix supp:<http://www.w3.org/ns/supp#>",
    "rdf":"@prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
}
CATAGORY_DICT={
    2:"content",
    3:"carrier",
    0:"attribute",
    1:"relation"

}
def getRdf(fname):
    with open("./static/"+fname,'r',encoding='utf-8') as fp:
        json_data=json.load(fp)
        return json_data
def writeResultFile(fname,result):
    with open("./static/"+fname,'w+',encoding='utf-8') as fp:
        fp.write(result)
def main():
    structed_lang=""
    rdf=getRdf("rdf.json")
    prefix_used_list=[]
    for link in rdf['links']:
        if link.get('label') and link['label']['formatter'].find(':')>0:
            prefix=link['label']['formatter'].split(":")[0]
            if prefix not in prefix_used_list:
                structed_lang+="%s\n"%PREFIX_DICT[prefix]
                prefix_used_list.append(prefix)

    # for item_idx in range(len(rdf['categories'])):
    for item_idx in CATAGORY_DICT.keys():
        item_info=":%s{\n"%CATAGORY_DICT[item_idx]
        # sovle specially in content layer
        if item_idx==2:
            status_id_list=[]
            for node in rdf['nodes']:
                if node['category']==item_idx and node['name']!="" and node['id'] not in status_id_list:
                    item_info+="    %s:%s\n"%("rdf:subject",node['name'])
                    for link in rdf['links']:
                        if link['source']==node['id']:
                            item_info+="    %s:%s\n"%("rdf:predicate",link['label']['formatter'])
                            item_info+="    %s:%s\n"%("rdf:object",rdf['nodes'][int(link['target'])]['name'])
                            status_id_list.append(link['source'])
                            status_id_list.append(link['target'])
            item_info+="}\n"
        else:
            for link in rdf['links']:
                if int(link['source'])==item_idx:
                    if item_idx in [0,1]:
                        prefix="objectunit"
                    else:
                        prefix=CATAGORY_DICT[2]
                    item_info+="    :%s %s:%s\n"%(prefix,link['label']['formatter'],rdf['nodes'][int(link['target'])]['name'])
            item_info+="}\n"
        structed_lang+=item_info
    print(structed_lang)
    writeResultFile("RdfLanguage.txt",structed_lang)

if __name__=="__main__":
    main()

    # 多级转向压裂技术 压开 新的分支裂缝
    # 多级转向压裂技术 沟通 更多微裂缝
    # 多级转向压裂技术 增大 油层泄油面积
    # 多级转向压裂技术 提高 新疆油田X区块采收率