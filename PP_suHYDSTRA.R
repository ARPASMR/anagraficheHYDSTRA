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

dir_output<-"/home/meteo/programmi/anagrafica/"
dir_anagrafica<-"/home/meteo/programmi/anagrafica/"
fileout<-paste(dir_output,"check_PP_validaz_HYDSTRA.txt",sep="")

cat(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><  \n\n",file=fileout)
#==============================================================================
#   LEGGO INFO DI HYDSTRA - tutte
#==============================================================================
cat( "\n\n  > leggi informazioni di Hydstra SITE\n", file=fileout,append=TRUE)

HYD_SITE <- read.csv ( paste(dir_anagrafica,"SITE.CSV",sep="") , header = TRUE , dec=",", quote="\"", na.string=c("-9999",""),as.is = TRUE, sep=",") 
HYD_SITE_staz <- HYD_SITE$CATEGORY8[which(is.na(HYD_SITE$CATEGORY8)==F)]
HYD_SITE_nome <- HYD_SITE$STNAME[which(is.na(HYD_SITE$CATEGORY8)==F)]
HYD_SITE_nome2 <- HYD_SITE$STATION[which(is.na(HYD_SITE$CATEGORY8)==F)]
HYD_SITE_nome3 <- HYD_SITE$SHORTNAME[which(is.na(HYD_SITE$CATEGORY8)==F)]
#
cat( "  > leggi informazioni di Hydstra\n", file=fileout,append=TRUE)
HYD_anag <- read.csv ( paste(dir_anagrafica,"Anagrafica.csv",sep="") , header = TRUE , dec=",", quote="\"", as.is = TRUE,na.strings = c("-9999",""), sep=";") 
HYD_staz <- HYD_anag$IdStazione[which(is.na(HYD_anag$IdStazione)==F)]
HYD_nome <- HYD_anag$Nome[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP1 <- HYD_anag$IdPluv[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP2 <- HYD_anag$IdPluv2[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP <- c(HYD_PP1,HYD_PP2)

#==============================================================================
#   LEGGO INFO DEL DBmeteo - pluviometri da validare
#==============================================================================
cat( "  > leggi informazioni del DBmeteo\n", file=fileout,append=TRUE)

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
AND DataInizio<'2019-01-01'
AND (DataFine>'2018-01-01' OR DataFine is NULL)"), silent=TRUE)

DBmeteo_rete<-DBmeteo$IDrete
DBmeteo_sens<-DBmeteo$IDsensore
DBmeteo_staz<-DBmeteo$IDstazione
DBmeteo$Attributo[which(is.na(DBmeteo$Attributo)==T)]=""
DBmeteo_nome<-paste(DBmeteo$Comune," ",DBmeteo$Attributo)

####################### RICERCA STAZIONI NON IN HYDSTRA SITE 

cat("\n\n -------------------------------------------------------------------\n",file=fileout,append=T)
cat("\n Ricerca STAZIONI appartenenti al DBmeteo ma non appartenenti ad HYDSTRA SITE\n",file=fileout,append=T)
aux<-DBmeteo_staz %in% HYD_SITE_staz 
if (length(DBmeteo_nome[!aux])>0) {
cat("\n stazioni trovate: ", length(DBmeteo_staz[!aux]),"\n", file=fileout,append=T)
  cat("\n Rete, IDstaz, Nome\n", file=fileout,append=T)
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

####### RICERCA STAZIONI NON IN HYDSTRA Anagrafica

cat("\n\n -------------------------------------------------------------------\n",file=fileout,append=T)
cat("\n Ricerca STAZIONI appartenenti al DBmeteo ma non appartenenti ad HYDSTRA Anagrafica\n",file=fileout,append=T)
aux<-DBmeteo_staz %in% HYD_staz
if (length(DBmeteo_nome[!aux])>0) {
cat("\n stazioni trovate : ", length(DBmeteo_staz[!aux]),"\n", file=fileout,append=T)
  cat("\n Rete, IDstaz, Nome\n", file=fileout,append=T)
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

####### RICERCA PLUVIOMETRI NON IN HYDSTRA Anagrafica   

cat("\n\n -------------------------------------------------------------------\n",file=fileout,append=T)
cat("\n Ricerca PLUVIOMETRI appartenenti al DBmeteo ma non appartenenti ad HYDSTRA Anagrafica\n",file=fileout,append=T)
aux<-DBmeteo_sens %in% HYD_PP
if (length(DBmeteo_nome[!aux])>0) {
cat("\n pluviometri trovati : ", length(DBmeteo_staz[!aux]),"\n", file=fileout,append=T)
  cat("\n Rete, IDstaz, Nome, IDsens\n", file=fileout,append=T)
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

####### VERIFICA NOMI IN HYDSTRA SITE

cat("\n\n -------------------------------------------------------------------\n",file=fileout,append=T)
cat("\n Verifica dei nomi in HYDSTRA SITE\n",file=fileout,append=T)
cat("\n IDstaz, IDsens,HYD_SITE_nome,HYD_SITE_nome2, HYD_SITE_nome3,  DBmeteo_nome\n", file=fileout,append=T)
i<-1
while(i<=length(DBmeteo_staz)) {
  j<-which(HYD_SITE_staz==DBmeteo_staz[i])

  if (length(j)!=1) {
  cat("in HYDSTRA ID=", DBmeteo_staz[i], " non esiste\n", file=fileout,append=T)
  } else {
   cat(DBmeteo_staz[i],
       ",",
       DBmeteo_sens[i],
       ",",
       HYD_SITE_nome[j],
       ",",
       HYD_SITE_nome2[j],
       ",",
       HYD_SITE_nome3[j],
       ",",
       DBmeteo_nome[i]  ,"\n",file=fileout,append=T)
  }
  i <- i + 1
}

####### VERIFICA NOMI E ASSOCIAZIONE STAZIONI/SENSORI IN HYDSTRA Anagrafica
HYD_staz <- HYD_anag$IdStazione[which(is.na(HYD_anag$IdStazione)==F)]
HYD_nome <- HYD_anag$Nome[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP1 <- HYD_anag$IdPluv[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP2 <- HYD_anag$IdPluv2[which(is.na(HYD_anag$IdStazione)==F)]
HYD_PP <- c(HYD_PP1,HYD_PP2)

cat("\n\n -------------------------------------------------------------------\n",file=fileout,append=T)
cat("\n Verifica dei nomi in HYDSTRA Anagrafica\n",file=fileout,append=T)
cat("\n IDsens, HYD_nome,  DBmeteo_nome, HYD_staz, DBmeteo_staz\n", file=fileout,append=T)
i<-1
while(i<=length(DBmeteo_sens)) {
  j<-which(HYD_PP==DBmeteo_sens[i])

  if (length(j)!=1) {
  cat("in HYDSTRA ID=", DBmeteo_sens[i], " non esiste\n", file=fileout,append=T)
  } else {
   cat(DBmeteo_sens[i],
       ",",
       HYD_nome[j],
       ",",
       DBmeteo_nome[i]  ,
       ",",
       HYD_staz[j],
       ",",
       DBmeteo_staz[i],"\n",file=fileout,append=T)
   if(is.na(DBmeteo_staz[i])==F && is.na(HYD_staz[j])==F) {
    if(DBmeteo_staz[i]!=HYD_staz[j])cat("\n\n\ ATTENZIONE, sensore ",DBmeteo_sens[i]," ha DBmeteo_IDstaz=",DBmeteo_staz[i]," ma HYDSTRA_IDstaz=",HYD_staz[j],"\n\n\n\n",file=fileout,append=T)
    }
  }
  i <- i + 1
}

comando <- paste ("mv " , fileout," /srv/www/htdocs/applications/controlli/")
try(system(comando, intern = TRUE))
#------------------------------------------------------------------------------
warnings()
quit(status=0)

