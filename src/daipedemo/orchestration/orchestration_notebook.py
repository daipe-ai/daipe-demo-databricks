# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestration notebook

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

import datalakebundle.imports as dl
from pyspark.dbutils import DBUtils

displayHTML("""
<!doctypehtml><title>Lineage</title><meta content=width=device-width,user-scalable=no,initial-scale=1,maximum-scale=1 name=viewport><script src=https://unpkg.com/cytoscape/dist/cytoscape.min.js></script><script src=https://unpkg.com/dagre@0.8.2/dist/dagre.js></script><script src=https://unpkg.com/cytoscape-dagre@2.3.2/cytoscape-dagre.js></script><style>body{font-family:helvetica;font-size:15px;height:2000px}#cy{width:100%;height:90%;z-index:999}h1{opacity:.5;font-size:1em;font-weight:bold}button{font-size:15px;margin-right:10px}.line{clear:left;height:25px;margin-top:6px;margin-right:6px;margin-bottom:6px}.radio{margin-left:25px}div.graph{height:1000px}</style><script>document.addEventListener("DOMContentLoaded",function(){var e=window.cy=cytoscape({container:document.getElementById("cy"),layout:{name:"dagre"},boxSelectionEnabled:!1,autounselectify:!0,style:[{selector:"node",style:{shape:"round-rectangle",label:"data(id)",width:"label",padding:"10px 20px 10px 20px","text-valign":"center","text-halign":"center","background-color":"#fff","font-size":"22px"}},{selector:"node[points]",style:{"shape-polygon-points":"data(points)",label:"polygon(custom points)","text-wrap":"wrap"}},{selector:'node[type = "inputDataset"]',style:{"background-color":"#FFFFE0"}},{selector:'node[type = "outputDataset"]',style:{"background-color":"#ADD8E6"}},{selector:'node[type = "function"]',style:{"background-color":"#C0C0C0"}},{selector:'node[type = "layer"]',style:{"background-color":"#036bfc",padding:"30px"}},{selector:'node[type = "feature"]',style:{"background-color":"#4CFF00"}},{selector:'node[layer_name = "bronze"]',style:{"background-color":"#cd7f32",padding:"30px"}},{selector:'node[layer_name = "silver"]',style:{"background-color":"#C0C0C0",padding:"30px"}},{selector:'node[layer_name = "gold"]',style:{"background-color":"#FFD700",padding:"30px"}},{selector:"edge",style:{"curve-style":"bezier","target-arrow-shape":"triangle",width:2,"line-color":"#000","target-arrow-color":"#000"}},{selector:":parent",css:{"text-valign":"top","text-halign":"center"}}],elements:{nodes:[{group:"nodes",data:{id:"bronze/tbl_loans",name:"bronze_tbl_loans",parent:"bronze",notebookId:"None",type:"notebook"}},{group:"nodes",data:{id:"bronze/tbl_repayments",name:"bronze_tbl_repayments",parent:"bronze",notebookId:"None",type:"notebook"}},{group:"nodes",data:{id:"silver/tbl_loans",name:"silver_tbl_loans",parent:"silver",notebookId:"None",type:"notebook"}},{group:"nodes",data:{id:"silver/tbl_repayments",name:"silver_tbl_repayments",parent:"silver",notebookId:"None",type:"notebook"}},{group:"nodes",data:{id:"silver/tbl_joined_loans_and_repayments",name:"silver_tbl_joined_loans_and_repayments",parent:"silver",notebookId:"None",type:"notebook"}},{group:"nodes",data:{id:"bronze",layer_name:"bronze",type:"layer"}},{group:"nodes",data:{id:"silver",layer_name:"silver",type:"layer"}}],edges:[{group:"edges",data:{source:"silver/tbl_loans",target:"silver/tbl_joined_loans_and_repayments"}},{group:"edges",data:{source:"bronze/tbl_loans",target:"silver/tbl_loans"}},{group:"edges",data:{source:"silver/tbl_repayments",target:"silver/tbl_joined_loans_and_repayments"}},{group:"edges",data:{source:"bronze/tbl_repayments",target:"silver/tbl_repayments"}}]}});e.makeLayout({name:"dagre",rankDir:"TB",nodeSep:150}).run()});</script><body><h1>Lineage</h1><div id=cy></div>
""")

# COMMAND ----------

@dl.notebook_function()
def orchestrate(dbutils: DBUtils):
    dbutils.notebook.run("./../bronze/tbl_loans", 0)
    dbutils.notebook.run("./../bronze/tbl_repayments/tbl_repayments", 0)
    dbutils.notebook.run("./../silver/tbl_loans", 0)
    dbutils.notebook.run("./../silver/tbl_repayments/tbl_repayments", 0)
    dbutils.notebook.run("./../silver/tbl_joined_loans_and_repayments", 0)

# COMMAND ----------


