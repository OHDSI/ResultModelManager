# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' @export
#' @title ExportHandler
#' @description
#' Basic file export handler for use in ResultExportManager. Default implementation writes csv files to disk
#'
ExportHandler <- R6::R6Class(
  classname = "ExportHandler",
  public = list(
    exportDir = "",
    initalize = function(exportDir) {
      self$exportDir <- exportDir
    },

    saveResultFile = function(data, dataFileName, append) {
      readr::write_csv(data, file = dataFileName, append = append)
      return(TRUE)
    }
  )
)

#' @export
#' @title S3 Export Handler
#' @description
#' Exports handler that saves results to amazon S3 buckets.
#'
#' For usage you should check the configuration of aws secure credentials with aws.signature which is normally as simple
#' as setting system wide environment variables for R:
#'
#' E.g.
#' Sys.setenv("AWS_ACCESS_KEY_ID" = "mykey",
#' "AWS_SECRET_ACCESS_KEY" = "mysecretkey",
#' "AWS_DEFAULT_REGION" = "us-east-1",
#' "AWS_SESSION_TOKEN" = "mytoken")
S3ExportHandler <- R6::R6Class(
  classname = "S3ExportHandler",
  inherit = ExportHandler,
  pivate = list(
    awsSseType = NULL,
    awsObjectKey = NULL,
    exportDir = ""
  ),
  public = list(
    logFile = "",
    #' @param logFile           Location of log file for uploads to interrogate for issues
    #' @param exportDir         (optional) s3 sub directory for files to be saved to e.g. filename.csv is saved to
    #'                          /<exportDir>/filename.csv within bucket. This is a good idea if you're running a study
    #'                          Across multiple databases
    #' @param bucket            s3 bucket to use
    initalize = function(logFile,
                         exportDir = "",
                         bucket = Sys.getenv("AWS_BUCKET_NAME"),
                         awsSseType = Sys.getenv("AWS_SSE_TYPE"),
                         awsObjectKey = Sys.getenv("AWS_OBJECT_KEY")) {
      checkmate::assertPathForOutput(logFile)
      checkmate::assertString(exportDir)
      checkmate::assertString(bucket)
      checkmate::assertString(awsSseType)
      checkmate::assertString(awsObjectKey)

      # Check if required s3 packages are installed
      s3PackagesInstalled <- all(c("aws.s3", "aws.ec2metadata", "aws.signature") %in% as.data.frame(installed.packages())$Package)
      if (!s3PackagesInstalled) {
        if (interactive()) {
          resp <- utils::menu(c("Yes", "No"), title = "Package aws.s3 is required to use this functionality. Install?")
          if (resp != 1) {
            stop("Must install aws.s3 to continue")
          }
        }
        install.packages(c("aws.signature", "aws.s3", "ec2metadata"))
      }
      # Check credentials are set/valid
      credentials <- aws.signature::locate_credentials()
      checkmate::assertFALSE(is.null(credentials$key))
      checkmate::assertFALSE(is.null(credentials$secret))
      checkmate::assertTRUE(aws.s3::bucket_exist(bucket))

      self$logFile <- logFile
      self$bucket <- bucket
      private$awsSseType <- awsSseType
      private$awsObjectKey <- awsObjectKey
      private$exportDir <- exportDir

      self
    },

    saveResultFile = function(data, dataFileName, append) {
      checksum <- digest::digest(data, "sha1")
      object <- paste(private$awsObjectKey, dataFileName, sep = "/")

      tryCatch({
        # TODO: Big one - how appending to files in buckets is handled
        success <- aws.s3::s3write_using(data,
                                         readr::write_csv,
                                         object = object,
                                         na = "",
                                         append = append,
                                         bucket = self$bucket,
                                         opts = list(
                                           check_region = FALSE,
                                           headers = list(`x-amz-server-side-encryption` = private$awsSseType),
                                           multipart = TRUE
                                         ))
      }, error = function (...) {
        if (private$stopOnError) {
          stop(...)
        }
        success <- FALSE
      })

      # Always store meta data logs locally
      log <- data.frame(object = object,
                        bucket = self$bucket,
                        ec2Meta = jsonlite::toJSON(aws.ec2metadata::instance_document()),
                        success = success,
                        checksum = checksum)
      readr::write_csv(log, file = config$awsS3Log, append = file.exists(self$logFile))

      return(success)
    }
  )
)