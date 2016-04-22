# base_dados_CVM

    # Descricao_etapas:
        
        # 1-) Rodar função "download_multiplo"
        #   n1 e n2: nºs inicial e final, respectivamente, dos documentos pesquisados
        #   Ex.: n1 = 1: doc de 2010; n2 = 50000: doc de 2015
        
        # 2-) Rodar função "download_CVM"
        #   df: dataframe gerado na etapa anterior (denominado "dtfr" ou "cvm_AAAAMMDD")
        #   tipo_doc: "dfp" ou "itr"
        #   data_ref: AAAMMDD
        #   nrow_inicio e nrow_fim: linhas inicial e final do dataframe (df) filtrado por tipodoc e dataref
        
        # 3-) Rodar função "col_PlanoConta", de modo a estruturar e corrigir conteúdo (caracteres) da coluna PlanoConta gerada na etapa anterior
        
        # 4-) Rodar função "select_last", que seleciona a última linha de cada DescricaoConta dos dados financeiros do período em análise
        #   df_dfin: dataframe contendo os dados brutos resultantes da etapa anterior
        #   vetor_PlanoConta: vetor contendo as descrições dos Planos de Contas (cod_PlanoConta ou cod_PlanoConta_simplific)
        
        # 5-) Usar função select (dplyr) para selecionar colunas de modo a gerar cada dataframe DFP_AAAA
        #   Ex.:    DFP_AAAA <- select(df_plano_conta, ccvm:ValorConta1)
        #           DFP_(AAAA-1) <- select(df_plano_conta, ccvm:DescricaoConta, ValorConta2)
        
        # 6-) Carregar packages "reshape" e "reshape2" para gerar pivot tables onde:
        #       linhas: ccvm
        #       colunas: contas (PlanoConta) do BP/DRE/DFC
        #       valores: valores das contas
        
        # 6.1-) Gerar pivot table:
        #   DPF_AAAA <- cast(data = DFP_AAAA, formula = ccvm ~ PlanoConta, value = "ValorContaN")
        
        # 6.2-) Estruturar dataframe relacionando cada PlanoConta à respectiva DescricaoConta (inclusive para setor financeiro):
        #   Obs.: essa dataframe não precisa ser gerada sempre, podendo ser reaproveitada no environment até alguma mudança na estrutura proposta pelo IFRS
        #   DPF_AAAA_colnames <- cast(data = DFP_AAAA, formula = PlanoConta ~ DescricaoConta)
        #   Eliminar zeros: DFP_AAAA_colnames[DFP_AAAA_colnames==0] <- NA
        #   Importar DFP_AAAA_colnames para Excel (função write.xlsx2) e tratar arquivo, de modo a gerar dataframe com 3 colunas: PlanoConta, DescricaoConta e DescricaoConta_Bancos
        #   Após tratamento, retornar dataframe para o R (função read.xlsx2) com nome PlConta_DescrConta
        
        # 6.3-) Renomear colunas da dataframe substituindo PlanoConta por DescricaoConta:
        #   DFP_AAAA <- setnames(x = DFP_AAAA, old = names(DFP_AAAA), new = c("ccvm", as.vector(PlConta_DescrConta[,2])))
        #   Obs.: os nomes das colunas do setor financeiro que diferem das denominações das empresas dos demais setores podem ser editados manualmente
        
        # 7-) (opcional) Rodar função "df_dfin", que cria tabelas de dados financeiros mesclando dados com tabela "empresas"
        #   df_dfin: dataframe oriunda da etapa anterior, contendo os dados financeiros em análise
        #   Obs.: pode ser feito diretamente no Access, inclusive somente ao final da geração dos índices de avaliação
        
        # 8-) Exportar dados para Access ou Excel:
        
        # Access:
        
        # Carregar package RODBC:
        # library(RODBC)
        
        # Conectar com arquivo Access (*.mdb):
        # ch_Access <- odbcConnectAccess(access.file = "D:/Users/TPBAR/Documents/R/CVM/DFP_v3.mdb")
        
        # Exportar dados do R para o Access:
        # RODBC::sqlSave(channel = ch_Access, dat = dataframe)
        # Ou
        # Importar dados do Access para o R:
        # RODBC::sqlFetch(channel = ch_Access, sqtable = Access_obj)
        
        # Fechar conexão:
        # odbcClose(channel = ch_Access)
        
        # Excel:
        
        # Carregar package xlsx:
        # library(xlsx)
        
        # Exportar dados do R para o Excel:
        # write.xlsx2(x = dataframe, file = output_path, sheetName = , col.names = T, row.names = F)
        # Ou
        # Importar dados do Excel para o R:
        # read.xlsx2(file = input_path, ...)
  
    download_multiplo <- function(n1, n2){
      # n1 e n2: ndoc início e fim
      
      # LEMBRAR DE ALTERAR DIRETÓRIO DE TRABALHO (setwd())
      
      # Carrega packages:
      library(data.table)
      library(dplyr)
      library(XML)
      
      # Cria vetores para armazenamento dos dados:
      endereco <- vector(length = n2-n1+1)
      arquivo <- vector(length = n2-n1+1)
      ccvm <- vector(length = n2-n1+1)
      tipodoc <- vector(length = n2-n1+1)
      dataref <- vector(length = n2-n1+1)
      
      # Estrutura endereço do link:
      # end <- "http://www.rad.cvm.gov.br/ENETCONSULTA/frmDownloadDocumento.aspx?CodigoInstituicao=1&NumeroSequencialDocumento="
      end <- "https://www.rad.cvm.gov.br/ENETCONSULTA/frmDownloadDocumento.aspx?CodigoInstituicao=1&NumeroSequencialDocumento="
      
      # Nomeia arquivo baixado:
      filename <- "download.zip"
      
      # Define contador para armazenamento dos dados:
      j <- 0
      
      # Registra instante de início:
      #    Ti <- proc.time()
      
      for (i in (n1:n2)){
          
          # Exibe nº i para cada ciclo da rodada:
          show(i)
          
          # Baixa dados CVM:
          try(expr = {
              download.file(paste0(end, i), destfile = filename, mode = "wb", quiet = T)
          }, silent = T)
          
          # Testa se há conteúdo no arquivo 'download.zip' e executa procedimentos caso exista:
          if (file.size(filename) > 4096){
              
              # Descompacta arquivo:
              unzip(zipfile = filename)
              
              # Remove arquivo "*.zip":
              file.remove(filename)
              
              # Seleciona dados do arquivo baixado para estruturar a lista/data frame:
              
              # Lista arquivos descompactados:
              allfiles <- list.files()
              
              # Lista arquivos .xml:
              xml_files <- list.files(pattern = ".xml")
              
              # Lista arquivos correspondentes ao tipo de documento enviado pelo link da CMV:
              if (length(allfiles[!allfiles %in% xml_files]) == 0) {
                  
                  # Atualiza contador:
                  j <- j+1
                  
                  # Armazena dados nos vetores previamente criados:
                  endereco [j] <- NA
                  arquivo [j] <- NA
                  ccvm[j] <- NA
                  tipodoc[j] <- NA
                  dataref[j] <- NA
              }
              
              # Lista arquivos correspondentes ao tipo de documento enviado pelo link da CMV:
              if (length(allfiles[!allfiles %in% xml_files]) == 1){
                  arq <- allfiles[!allfiles %in% xml_files]
                  
                  # Cria vetores:
                  codcvm <- substr(x = arq, start = 1, stop = 6)
                  dtref <- substr(x = arq, start = 7, stop = 14)
                  tdoc <- substr(x = arq, start = nchar(arq)-2, stop = nchar(arq))
                  
                  # Atualiza contador:
                  j <- j+1
                  
                  # Armazena dados nos vetores previamente criados:
                  endereco [j] <- paste0(end, i)
                  arquivo [j] <- arq
                  ccvm[j] <- codcvm
                  tipodoc[j] <- tdoc
                  dataref[j] <- dtref
              }
              
              # Lista arquivos correspondentes ao tipo de documento enviado pelo link da CMV:
              if (length(allfiles[!allfiles %in% xml_files]) > 1){
                  fca_files <- list.files(pattern = ".fca")
                  arq <- allfiles[!allfiles %in% c(xml_files, fca_files)]
                  
                  # Cria vetores:
                  codcvm <- substr(x = arq, start = 1, stop = 6)
                  dtref <- substr(x = arq, start = 7, stop = 14)
                  tdoc <- substr(x = arq, start = nchar(arq)-2, stop = nchar(arq))
                  
                  # Atualiza contador:
                  j <- j+1
                  
                  # Armazena dados nos vetores previamente criados:
                  endereco [j] <- paste0(end, i)
                  arquivo [j] <- arq
                  ccvm[j] <- codcvm
                  tipodoc[j] <- tdoc
                  dataref[j] <- dtref
              }
          }
          
          # Testa se há conteúdo no arquivo 'download.zip' e executa procedimentos caso NÃO exista:
          else {
              
              # Atualiza contador:
              j <- j+1
              
              # Armazena dados nos vetores previamente criados:
              endereco [j] <- NA
              arquivo [j] <- NA
              ccvm[j] <- NA
              tipodoc[j] <- NA
              dataref[j] <- NA
              
              # Exclui arquivo 'download.zip':
              file.remove(filename)
          }
          
          # Exclui arquivos:
          file.remove(list.files(pattern = ".xml"))
          file.remove(list.files(pattern = ".zip"))
          file.remove(list.files(pattern = ".fca"))
          file.remove(list.files(pattern = ".fre"))
          file.remove(list.files(pattern = ".dfp"))
          file.remove(list.files(pattern = ".itr"))
          file.remove(list.files(pattern = ".sec"))
          file.remove(list.files(pattern = ".ipe"))
      }
      
      # Cria vetor ndoc:
      # ndoc <- substr(x = endereco, start = 112, stop = 116)
      ndoc <- substr(x = endereco, start = 113, stop = 117)
      
      # Cria e retorna lista/data frame consolidando vetores:
      lista_vetores <<- list(endereco, arquivo, ccvm, tipodoc, dataref, ndoc)
      dtfr <<- data.frame(endereco = lista_vetores[[1]],
                          arquivo = lista_vetores[[2]],
                          ccvm = lista_vetores[[3]],
                          tipodoc = lista_vetores[[4]],
                          dataref = lista_vetores[[5]],
                          ndoc = lista_vetores[[6]])
      
      # Registra instante do término da rodada do algoritmo:
      #    Tf <- proc.time()
      # Retorna tempo decorrido (em segundos):
      #    Td <- Tf-Ti
      #    T_decorrido <<- Td[3]
      
      # Salva arquivo .RData ao final da rodada, de modo a salvar os objetos gerados:
      save.image("D:/Users/TPBAR/Documents/R/dtfr_CVM.RData")
    }

    download_CVM <- function(df, tipo_doc, data_ref, nrow_inicio, nrow_fim){
    
      # df: dataframe contendo os endereços dos documentos a serem baixados (cvm_AAAAMMDD ou itr_dfp_AAAAMMDD)
      # tipo_doc: docs a serem baixados ("itr", "dfp")
      # data_ref: data referente ao período contábil dos documentos (AAAAMMDD)
      # nrow_inicio e nrow_fim: linhas do dataframe df
      
      # Carrega packages necessários:
      require(dplyr)
      require(XML)
      require(data.table)
      require(tidyr)
      require(xlsx)
      
      # Define diretório de trabalho:
      # setwd(dir = "./R/download_multiplo/")
      
      # Seleciona tipo de documento de interesse e data de referência:
      df <- filter(df, tipodoc == tipo_doc, dataref == data_ref)
      
      # Insere coluna ndoc:
      #ndoc <- substr(x = as.character(df[,1]), start = 112, stop = 116)
      #df <- cbind(df, ndoc)
      
      # Seleciona somente o último endereço para situações em que o ccvm se repete na df:
      dt <- data.table(df)
      setkey(dt, ccvm)
      df <- dt[J(unique(ccvm)), mult = "last"]
      df <- as.data.frame(df)
      
      # Cria data frame para armazenamento dos dados brutos:
      dados_fin <- data.frame()
      
      # Criação de data tables para Ativo, Passivo, DRE e DFC:
      ativo <- tbl_dt(data.table())
      passivo <- tbl_dt(data.table())
      DRE <- tbl_dt(data.table())
      DFC <- tbl_dt(data.table())
      
      # Extração de dados do arquivo .zip baixado da CVM:
      for (i in nrow_inicio:nrow_fim) {
          
          show(i)
          
          # Nomeia arquivo:
          filename <- paste0(substr(as.character(df[i,2]), start = 1, stop = 18), ".zip")
          
          # Baixa arquivo:
          try(expr = {
              download.file(url = as.character(df[i,1]), destfile = filename, quiet = T, mode = "wb")
          }, silent = T)
          
          # Descompacta conteúdo baixado:
          unzip(zipfile = filename)
          
          # Remove arquivo "*.zip":
          file.remove(filename)
          
          # Renomeia extensão do arquivo "*.itr" para "*.zip":
          try(expr = {
              file.rename(from = list.files(pattern = ".itr"), to = filename)
          }, silent = T)
          # Obs.: a função "try" permite igorar erros (neste caso, ignora o erro que aponta inexistência de arquivo com extensão .itr)
          
          # Renomeia extensão do arquivo "*.dfp" para "*.zip":
          try(expr = {
              file.rename(from = list.files(pattern = ".dfp"), to = filename)
          }, silent = T)
          
          # Descompacta conteúdo da nova pasta:
          unzip(zipfile = filename)
          
          # Transfere dados para data frame:
          dados <- xmlToDataFrame("InfoFinaDFin.xml", 
                                  colClasses = c("numeric",
                                                 "character",
                                                 "character",
                                                 "character",
                                                 "character",
                                                 "character",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric",
                                                 "numeric"),
                                  stringsAsFactors = F)
          
          # Alterar caracteres UTF-8 para ISO:
          DescricaoConta <- iconv(x = dados$DescricaoConta1, from = "UTF-8", to = "ISO-8859-1")
          
          # Consolida informações financeiras:
          info_fin <- cbind(dados, DescricaoConta)
          info_fin <- select(info_fin,
                             PlanoConta,
                             DescricaoConta,
                             ValorConta1:ValorConta12) %>%
              mutate(ccvm = df[i,3],
                     doc = df[i,4],
                     dataref = df[i,5],
                     ndoc = df[i,6]) %>%
              select(ccvm,
                     PlanoConta,
                     DescricaoConta,
                     ValorConta1:ValorConta12,
                     doc,
                     dataref,
                     ndoc)
          
          # Registra novas info_fin nos dados brutos:
          dados_fin <- rbind(dados_fin, info_fin)
          
          # Exclui arquivos:
          file.remove(list.files(pattern = ".xml"))
          file.remove(list.files(pattern = ".zip"))
          file.remove(list.files(pattern = ".fca"))
          file.remove(list.files(pattern = ".fre"))
          file.remove(list.files(pattern = ".dfp"))
          file.remove(list.files(pattern = ".itr"))
          file.remove(list.files(pattern = ".sec"))
          file.remove(list.files(pattern = ".ipe"))
          
      # Retorna matriz dos dados financeiros coletados no environment:
      dados_financ <<- dados_fin
      }
    }

    col_PlanoConta <- function(matriz){
      # onde matriz = data.frame onde estão os dados financeiros em análise (18 colunas)
      
      # Estruturar e corrigir dados da coluna PlanoConta:
      
        # Ajustar dados da coluna PlanoConta:
        PlanoConta1 <- substr(x = matriz[,2], start = 9, stop = max(nchar(matriz[,2])))
        PlanoConta1 <- sub(pattern = "falsefalsetrue", replacement = "", x = PlanoConta1)
        PlanoConta1 <- sub(pattern = "true", replacement = "_", x = PlanoConta1)
        PlanoConta1 <- sub(pattern = "false", replacement = "_", x = PlanoConta1)
        PlanoConta1 <- gsub(pattern = "_.*?$", replacement = "", x = PlanoConta1)
        
        # This regular expression matches:
        # the end of the string ($) (if beginning would be ^);
        # any character (.) repeated zero or more times (*);
        # underscore (_);
        # ? makes the underscore reference "lazy", undefining the distance far from the "_"
        
        # Insere coluna PlanoConta1 no dataframe:
        dados_financ2 <- cbind(matriz, PlanoConta1)
        
        # Reordena ordem das colunas:
        dados_financ2 <- select(dados_financ2, ccvm, PlanoConta1, DescricaoConta:ndoc)
        
        # Renomear coluna PlanoConta1 para PlanoConta:
        colnames(x = dados_financ2) [2] <- "PlanoConta"
        
        # Exclui levels inutilizados:
        dados_financ2 <- droplevels.data.frame(dados_financ2)
        
        # Substitui dataframe anterior:
        dados_financ <<- dados_financ2
    }

    select_last <- function(df_dfin, vetor_PlanoConta){
    
    # Seleciona último registro da coluna PlanoConta dos dataframes consolidados
        # Onde:
            # df_dfin: dataframe contendo os dados brutos
            # vetor_PlanoConta: vetor contendo os Planos de Contas a serem utilizados no algoritmo (cod_PlanoCOnta ou cod_PlanoConta_simplific)
        
        # Carrega packages:
        require(data.table)
        require(dplyr)
        
        # Cria datatable para armazenar dados:
        dt_plconta <- data.table()
        
        # Roda função de seleção do último registro:
        
        for (i in (1:length(vetor_PlanoConta))){
            
            # Filtra dataframe pelo nº do PlanoConta:
            dt <- filter(df_dfin, PlanoConta==vetor_PlanoConta[i])
            
            # Converte dataframe em datatable:
            dt <- data.table(dt)
            
            # Para cada ccvm, retorna apenas o último registro:
            df <- dt[, .SD[c(.N,.N)], by=ccvm]
            setkey(df, ccvm)
            dt_plconta <- rbind(dt_plconta, df[J(unique(ccvm)), mult = "last"])
        }
        
        # Converte datatable consolidatória em dataframe:
        df_plconta <- data.frame(dt_plconta)
        
        # Remove levels inexistentes:
        df_plconta <- droplevels.data.frame(df_plconta)
        
        # Reordena dados da dataframe consolidatória por ccvm e por PlanoConta:
        df_plconta <- arrange(df_plconta, ccvm, PlanoConta)
        
        # Retorna dataframe com os dados consolidados:
        df_plano_conta <<- df_plconta
    }

