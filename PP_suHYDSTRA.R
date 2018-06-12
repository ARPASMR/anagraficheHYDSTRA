###############################################################################
#  
library(DBI)
library(RMySQL)
library(RODBC)
#
#+ gestione dell'errore
neverstop<-function(){
  print("EE..ERRORE durante l'esecuzione dello script!! Messaggio d'Errore prodotto:\n")
  quit()
}
options(show.error.messages=TRUE,error=neverstop)

dir_output<-"/home/meteo/programmi/anagrafica/out/"
dir_anagrafica<-"/home/meteo/programmi/anagrafica/"
fileout<-paste(dir_output,"check_PP_altri_HYDSTRA.txt",sep="")

cat(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><  \n\n",file=fileout)
#==============================================================================
#   LEGGO INFO DI HYDSTRA - tutte
#==============================================================================
cat( "\n\n  > leggi informazioni di Hydstra\n", file=fileout,append=TRUE)

HYD_SITE <- read.csv ( paste(dir_anagrafica,"SITE.CSV",sep="") , header = TRUE , dec=",", quote="\"", na.string=c("-9999",""),as.is = TRUE, sep=",") 
HYD_SITE_staz <- HYD_SITE$CATEGORY8[which(is.na(HYD_SITE$CATEGORY8)==F)]
HYD_SITE_nome <- HYD_SITE$STNAME[which(is.na(HYD_SITE$CATEGORY8)==F)]
#
HYD_anag <- read.csv ( paste(dir_anagrafica,"Anagrafica.csv",sep="") , header = TRUE , dec=",", quote="\"", as.is = TRUE,na.strings = c("-9999",""), sep=";") 
HYD_staz <- HYD_anag$IdStazione[which(is.na(HYD_anag$IdStazione)==F)]
HYD_nome <- HYD_anag$Nome[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP1 <- HYD_anag$IdPluv[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP2 <- HYD_anag$IdPluv2[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP <- c(HYD_PP1,HYD_PP2)

#==============================================================================
#   LEGGO INFO DEL DBmeteo - pluviometri da validare
#==============================================================================
cat( "\n\n  > leggi informazioni del DBmeteo\n", file=fileout,append=TRUE)

MySQL(max.con=16,fetch.default.rec=500,force.reload=FALSE)
drv<-dbDriver("MySQL")
conn<-try(dbConnect(drv,group="Visualizzazione"))
if (inherits(conn,"try-error")) {
  print( "ERRORE nell'apertura della connessione al DBmeteo \n")
  print( " Eventuale chiusura connessione malriuscita ed uscita dal programma \n")
  dbDisconnect(conn)
  rm(conn)
  dbUnloadDriver(drv)
  quit(status=1)
}
DBmeteo<-try(dbGetQuery(conn,"SET NAMES utf8"), silent=TRUE)
DBmeteo<-try(dbGetQuery(conn, "
SELECT
IDrete,A_Stazioni.IDstazione, A_Sensori.IDsensore, Attributo,IDsensore,Comune, Provincia 
FROM
A_Stazioni,A_Sensori
WHERE A_Stazioni.IDstazione=A_Sensori.IDstazione
AND IDrete in (1,4)
AND NOMEtipologia ='PP'
AND DataInizio<'2018-01-01'
AND (DataFine>'2017-01-01' OR DataFine is NULL)"), silent=TRUE)

DBmeteo_rete<-DBmeteo$IDrete
DBmeteo_sens<-DBmeteo$IDsensore
DBmeteo_staz<-DBmeteo$IDstazione
DBmeteo$Attributo[which(is.na(DBmeteo$Attributo)==T)]=""
DBmeteo_nome<-paste(DBmeteo$Comune," ",DBmeteo$Attributo)

####################### RICERCA STAZIONI NON IN HYDSTRA SITE 

cat("\n Ricerca STAZIONI appartenenti al DBmeteo ma non appartenenti ad HYDSTRA SITE\n",file=fileout,append=T)
aux<-DBmeteo_staz %in% HYD_SITE_staz 
if (length(DBmeteo_nome[!aux])>0) {
cat("\n stazioni trovate: ", length(DBmeteo_staz[!aux]),"\n", file=fileout,append=T)
  cat("\n IDstazione, Nome\n", file=fileout,append=T)
  iii<-1
  while(iii<length(DBmeteo_staz[!aux])+1){
   if(DBmeteo_rete[!aux][iii]==1)DBmeteo_rete[!aux][iii]="Aria"
   if(DBmeteo_rete[!aux][iii]==4)DBmeteo_rete[!aux][iii]="INM"
   if(DBmeteo_rete[!aux][iii]==2)DBmeteo_rete[!aux][iii]="CMG"
   if(DBmeteo_rete[!aux][iii]==5)DBmeteo_rete[!aux][iii]="extraLomb"
   if(DBmeteo_rete[!aux][iii]==6)DBmeteo_rete[!aux][iii]="altro"
   cat(as.vector(DBmeteo_rete[!aux][iii]),
       ",",
       as.vector(DBmeteo_staz[!aux][iii]),
       ",",
       as.vector(DBmeteo_nome[!aux][iii])  ,"\n",file=fileout,append=T)
  iii<-iii+1
  }
} else {
  cat("\nstazioni trovate 0\n",file=fileout,append=T)
}

####### RICERCA STAZIONI E PLUVIOMETRI NON IN HYDSTRA Anagrafica   

cat("\n Ricerca STAZIONI/PLUVIOMETRI appartenenti al DBmeteo ma non appartenenti ad HYDSTRA Anagrafica\n",file=fileout,append=T)
aux<-DBmeteo_sens %in% HYD_PP
if (length(DBmeteo_nome[!aux])>0) {
cat("\n pluviometri trovati : ", length(DBmeteo_staz[!aux]),"\n", file=fileout,append=T)
  cat("\n IDstazione, Nome\n", file=fileout,append=T)
  iii<-1
  while(iii<length(DBmeteo_staz[!aux])+1){
   if(DBmeteo_rete[!aux][iii]==1)DBmeteo_rete[!aux][iii]="Aria"
   if(DBmeteo_rete[!aux][iii]==4)DBmeteo_rete[!aux][iii]="INM"
   if(DBmeteo_rete[!aux][iii]==2)DBmeteo_rete[!aux][iii]="CMG"
   if(DBmeteo_rete[!aux][iii]==5)DBmeteo_rete[!aux][iii]="extraLomb"
   if(DBmeteo_rete[!aux][iii]==6)DBmeteo_rete[!aux][iii]="altro"
   cat(as.vector(DBmeteo_rete[!aux][iii]),
       ",",
       as.vector(DBmeteo_staz[!aux][iii]),
       ",",
       as.vector(DBmeteo_nome[!aux][iii]),
       ",",
       as.vector(DBmeteo_sens[!aux][iii])  ,"\n",file=fileout,append=T)
  iii<-iii+1
  }
} else {
  cat("\npluviometri trovati 0\n",file=fileout,append=T)
}


#------------------------------------------------------------------------------
warnings()
quit(status=0)

