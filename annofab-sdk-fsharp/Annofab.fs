namespace Adacola.Annofab

open System
open FSharp.Data
open System.IO
open ICSharpCode.SharpZipLib

module Annofab =
    type AnnofabConfig = JsonProvider<"""[{"api_base_uri":"a","credentials":{"user_id":"a","password":"a"},"email":"str"}]""", SampleIsList = true>

    type Token = {
        IDToken : string
        AccessToken : string
        RefreshToken : string
    }

    exception ResourceNotFoundException of message : string * token : Token with
        override this.Message with get() = this.message

    exception AlreadyUpdatedException of message : string * token : Token with
        override this.Message with get() = this.message

    [<RequireQualifiedAccess>]
    type TaskPhase = Annotation | Inspection | Acceptance
    with
        override this.ToString() =
            match this with
            | Annotation -> "annotation"
            | Inspection -> "inspection"
            | Acceptance -> "acceptance"

    [<RequireQualifiedAccess>]
    type TaskStatus = NotStarted | Working | OnHold | Break | Complete | Rejected
    with
        override this.ToString() =
            match this with
            | NotStarted -> "not_started"
            | Working -> "working"
            | OnHold -> "on_hold"
            | Break -> "break"
            | Complete -> "complete"
            | Rejected -> "rejected"

    type ErrorResponse = JsonProvider<"""{"errors":[{"error_code":"EXPIRED_TOKEN","message":"The incoming token has expired"}]}""">
    type LoginRequest = JsonProvider<"""{"user_id":"str","password":"str"}""">
    type LoginResponse = JsonProvider<"""{"needs_to_change_password":false,"token":{"id_token":"str","refresh_token":"str","access_token":"str"}}""">
    type RefreshTokenRequest = JsonProvider<"""{"refresh_token": "str"}""">
    type RefreshTokenResponse = JsonProvider<"""{"id_token":"str","refresh_token":"str","access_token":"str"}""">
    type PutInputDataRequest = JsonProvider<"""{"input_data_name":"str","input_data_path":"str","input_data_type":"image","last_updated_datetime":"date"}""">
    type PutInputDataResponse = JsonProvider<"""{"input_data_id":"str","project_id": "str","input_data_name":"str","input_data_path":"str","original_input_data_path":"str","scale":0,"input_data_type":"image","updated_datetime":"date"}""">
    type GenerateTasksRequest = JsonProvider<"""{"allow_duplicate_input_data":true,"input_data_count":0,"input_data_order":"name_asc","task_id_prefix": "str"}""">
    type GenerateTasksResponse = JsonProvider<"""{"message":"str"}""">
    type GetInputsResponse = JsonProvider<"""{"list": [{"input_data_id":"str","project_id":"str","input_data_name":"str","input_data_path":"str","original_input_data_path":"str","scale":0,"input_data_type":"image","updated_datetime": "date"}],"has_next":true}""">
    type GetTaskResponse = JsonProvider<"""{"project_id":"str","task_id":"str","phase": "annotation","status": "not_started","input_data_id_list": ["str"],"account_id":"str","account_id_history": ["str"],"work_timespan":0,"start_datetime":"date","updated_datetime":"date"}""">
    type DeleteTaskResponse = JsonProvider<"""{"project_id":"str","task_id":"str","phase": "annotation","status": "not_started","input_data_id_list": ["str"],"account_id":"str","account_id_history": ["str"],"work_timespan": 0,"start_datetime":"date","updated_datetime":"date"}""">
    type PutTaskRequest = JsonProvider<"""{"input_data_id_list":["str"]}""">
    type PutTaskResponse = JsonProvider<"""{"project_id":"str","task_id":"str","phase":"annotation","status":"not_started","input_data_id_list": ["str"],"account_id":"str","account_id_history": ["str"],"work_timespan": 0,"start_datetime":"date","updated_datetime":"date"}""">
    type GetStatisticsTasksResponse = JsonProvider<"""[{"date":"date","tasks":[{"phase":"annotation","status":"not_started","count":1}]}]""">
    type GetStatisticsAccountsResponse = JsonProvider<"""[{"account_id":"string","histories":[{"date":"date","task_completed":1,"task_rejected":1,"worktime":"string"}]}]""">
    type GetTasksResponse = JsonProvider<"""{"list":[{"project_id":"str","task_id":"str","phase":"str","status":"str","input_data_id_list":["str"],"account_id":"str","account_id_history":["str","str"],"work_timespan":"468872","start_datetime":"date","updated_datetime":"date"}],"page_no":1,"total_page_no":10}""">
    type TrainingData = JsonProvider<"""{"project_id":"str","task_id":"str","input_data_id":"str","detail":[{"annotation_id":"str","user_id":"str","label_id":"str","label_name":{"messages":[{"lang":"ja-JP","message":"str"},{"lang":"en-US","message":"str"}],"default_lang":"ja-JP"},"data_holding_type":"inner","data":[],"additional_data_list":[],"comment":"str"}],"comment":"str","updated_datetime":"date"}""">
    type GetAnnotationResponse = JsonProvider<"""{"project_id":"str","task_id":"str","input_data_id":"str","detail":[],"comment":"str","updated_datetime":"str"}""">
    type PutAnnotationRequest = JsonProvider<"""{"project_id":"str","task_id":"str","input_data_id":"str","detail":[],"comment":"str","updated_datetime":"str"}""">
    type PutAnnotationResponse = JsonProvider<"""{"project_id":"str","task_id":"str","input_data_id":"str","detail":[],"comment":"str","updated_datetime":"str"}""">
    type GetMyAccountResponse = JsonProvider<"""{"account_id":"str","user_id":"str","username":"str","email":"str","reset_requested_email":"str","lang":"str","keylayout":"str","authority":"str","errors":"str","updated_datetime":"str"}""">
    type OperateTaskRequest = JsonProvider<"""{"status": "not_started","last_updated_datetime":"str","account_id":"str"}""">
    type OperateTaskResponse = JsonProvider<"""{"project_id":"str","task_id":"str","phase":"annotation","status":"not_started","input_data_id_list":["str"],"account_id":"str","account_id_history":["str"],"work_timespan":1,"number_of_rejections":1,"start_datetime":"str","updated_datetime":"str"}""">
    type AnnotationData = JsonProvider<"""{"project_id":"str","task_id":"str","input_data_id":"str","detail":[{"annotation_id":"str","user_id":"str","label_id":"str","label_name":{"messages":[{"lang":"ja-JP","message":"str"},{"lang":"en-US","message":"str"}],"default_lang":"ja-JP"},"data_holding_type":"inner","data":[],"additional_data_list":[],"comment":"str"}],"comment":"str","updated_datetime":"date"}""">

    let jsonHeaders = [HttpRequestHeaders.ContentType HttpContentTypes.Json]

    let getJsonAuthHeaders token = jsonHeaders @ ["Authorization", token.IDToken]

    let dateTimeOffsetToStr (dateTime : DateTimeOffset) = dateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffzzz")

    let strToDateTimeOffset dateTimeStr = DateTimeOffset.ParseExact(dateTimeStr, "yyyy-MM-ddTHH:mm:ss.fffzzz", null)

    let login (baseUri : Uri) (credentials : AnnofabConfig.Credentials) =
        let apiUri = Uri(baseUri, "login")
        let request = LoginRequest.Root(userId = credentials.UserId, password = credentials.Password)
        let body = request.JsonValue.ToString() |> HttpRequestBody.TextRequest
        let response =
            Http.RequestString(url = apiUri.AbsoluteUri, httpMethod = "POST", headers = jsonHeaders, body = body, responseEncodingOverride = "UTF-8")
            |> LoginResponse.Parse
        { IDToken = response.Token.IdToken; AccessToken = response.Token.AccessToken; RefreshToken = response.Token.RefreshToken }

    let refreshToken (baseUri : Uri) expiredToken =
        let apiUri = Uri(baseUri, "refresh-token")
        let request = RefreshTokenRequest.Root(refreshToken = expiredToken.RefreshToken)
        let body = request.JsonValue.ToString() |> HttpRequestBody.TextRequest
        let response =
            Http.RequestString(url = apiUri.AbsoluteUri, httpMethod = "POST", headers = jsonHeaders, body = body, responseEncodingOverride = "UTF-8")
            |> RefreshTokenResponse.Parse
        { IDToken = response.IdToken; AccessToken = response.AccessToken; RefreshToken = response.RefreshToken }

    let private trialCount = 10
    let private waitUnitMilliSecond = 1000

    let sendAndRefreshToken baseUri (sendApi : Token -> (HttpResponse * Token)) (parseResponse : HttpResponse -> 'a) token =

        let raiseError message (response : HttpResponse) =
            let errorMessage = 
                match response.Body with
                | Text text -> sprintf "%s ステータスコード : %d, body : %s" message response.StatusCode text
                | Binary _ -> sprintf "%s ステータスコード : %d" message response.StatusCode
            eprintfn "%s" errorMessage
            failwith errorMessage

        let rec loop token restTrialCount = 
            let response, token = sendApi token
            if response.StatusCode < 400 then parseResponse response, token else
            match response.StatusCode, response.Body with
            | 401, Text text ->
                printfn "Unauthorized"
                let errorResponse = text |> ErrorResponse.Parse
                if errorResponse.Errors.Length = 1 && errorResponse.Errors.[0].ErrorCode = "EXPIRED_TOKEN" then
                    eprintfn "トークン期限切れです。トークン再発行してから再度API実行します。残り試行回数 : %d" (restTrialCount - 1)
                    let newToken = refreshToken baseUri token
                    loop newToken (restTrialCount - 1)
                else raiseError "API呼び出しに失敗しました。" response
            | 404, Text text ->
                let message = sprintf "リソースが見つかりません。body : %s" text
                ResourceNotFoundException(message, token) |> raise
            | 409, Text text ->
                let message = sprintf "既に更新されています。body : %s" text
                AlreadyUpdatedException(message, token) |> raise
            | (500 | 502 | 504), Text text ->
                let errorResponse = text |> ErrorResponse.Parse
                match restTrialCount, errorResponse.Errors |> Array.toList with
                | 0, _ -> raiseError "API呼び出しに失敗しました。" response
                | _, error::errors ->
                    match error.ErrorCode with
                    | "INTERNAL_SERVER_ERROR" | "TIMEOUT" ->
                        let waitMilliSecond = waitUnitMilliSecond * pown 2 (trialCount - restTrialCount)
                        let cause = match error.ErrorCode with "TIMEOUT" -> "タイムアウト" | _ -> "エラー"
                        eprintfn "サーバ側で%sが発生しました。%d秒待った後リトライします。残り試行回数 : %d" cause (waitMilliSecond / 1000) (restTrialCount - 1)
                        error::errors |> List.iter (eprintfn "%O")
                        waitUnitMilliSecond * pown 2 (trialCount - restTrialCount) |> Async.Sleep |> Async.RunSynchronously
                        loop token (restTrialCount - 1)
                    | _ -> raiseError "API呼び出しに失敗しました。" response
                | _, [] -> raiseError "API呼び出しに失敗しました。" response
            | _ -> raiseError "API呼び出しに失敗しました。" response

        loop token trialCount

    let getTextResponse (response : HttpResponse) =
        match response.Body with
        | Text text -> text
        | Binary _ -> failwith "テキストのレスポンスを期待していましたがバイナリが返されました"

    let getBinaryResponse (response : HttpResponse) =
        match response.Body with
        | Binary binary -> binary
        | Text text -> failwithf "バイナリのレスポンスを期待していましたがテキストが返されました : %s" text

    let putInputData (baseUri : Uri) token projectID inputID (request : PutInputDataRequest.Root) =
        let apiUri = Uri(baseUri, sprintf "projects/%s/inputs/%s" projectID inputID)
        let body = request.JsonValue.ToString() |> HttpRequestBody.TextRequest
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "PUT", headers = getJsonAuthHeaders token, body = body, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> PutInputDataResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let generateTasks (baseUri : Uri) token projectID (request : GenerateTasksRequest.Root) =
        let apiUri = Uri(baseUri, sprintf "projects/%s/generate-tasks" projectID)
        let body = request.JsonValue.ToString() |> HttpRequestBody.TextRequest
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "POST", headers = getJsonAuthHeaders token, body = body, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> GenerateTasksResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getInputs (baseUri : Uri) token projectID (exclusiveStartID : string option) =
        let apiUri = Uri(baseUri, sprintf "projects/%s/inputs" projectID)
        let query = [exclusiveStartID |> Option.map (fun x -> "exclusive_start_id", x)] |> List.choose id
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, query = query, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> GetInputsResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getAllInputs (baseUri : Uri) token projectID =
        let rec loop result token exclusiveStartID =
            let response, token = getInputs baseUri token projectID exclusiveStartID
            let result = seq { yield! result; for x in response.List do yield x.InputDataId }
            if response.HasNext then
                match response.List |> Array.tryLast with
                | None -> failwith "has_nextがtrueにもかかわらずlistが空でした"
                | Some lastID -> lastID.InputDataId |> Some |> loop result token
            else result
        loop [] token None

    let getTask (baseUri : Uri) token projectID taskID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/tasks/%s" projectID taskID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> GetTaskResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let deleteTask (baseUri : Uri) token projectID taskID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/tasks/%s" projectID taskID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "DELETE", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> DeleteTaskResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let putTask (baseUri : Uri) token projectID taskID (request : PutTaskRequest.Root) =
        let apiUri = Uri(baseUri, sprintf "projects/%s/tasks/%s" projectID taskID)
        let body = request.JsonValue.ToString() |> HttpRequestBody.TextRequest
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "PUT", headers = getJsonAuthHeaders token, body = body, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> PutTaskResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getStatisticsTasks (baseUri : Uri) token projectID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/statistics/tasks" projectID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> GetStatisticsTasksResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getStatisticsAccounts (baseUri : Uri) token projectID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/statistics/accounts" projectID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> GetStatisticsAccountsResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getArchiveFull (baseUri : Uri) token projectID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/archive/full" projectID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse (response : HttpResponse) = response.Headers |> Map.find "Location" |> Uri
        let archiveUri, token = sendAndRefreshToken baseUri sendApi parseResponse token
        let archiveResponse = Http.Request(url = string archiveUri, httpMethod = "GET") |> getBinaryResponse
        (archiveResponse, archiveUri), token

    let getArchiveSimple (baseUri : Uri) token projectID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/archive/simple" projectID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse (response : HttpResponse) = response.Headers |> Map.find "Location" |> Uri
        let archiveUri, token = sendAndRefreshToken baseUri sendApi parseResponse token
        let archiveResponse = Http.Request(url = string archiveUri, httpMethod = "GET") |> getBinaryResponse
        (archiveResponse, archiveUri), token

    let getExtractedTrainingData extractedDir taskID inputDataID =
        if Directory.Exists extractedDir |> not then invalidArg "extractedDir" "ディレクトリが存在しません" else
        let filePath = Path.Combine(extractedDir, taskID, inputDataID + ".json")
        if File.Exists filePath then File.ReadAllText(filePath, Text.Encoding.UTF8) |> TrainingData.Parse |> Some else None

    let getInputData (baseUri : Uri) token projectID inputDataID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/inputs/%s/data" projectID inputDataID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse (response : HttpResponse) =
            let response =
                response.Headers |> Map.tryFind HttpResponseHeaders.Location |> Option.map (fun url -> Http.Request(url = url, httpMethod = "GET", responseEncodingOverride = "UTF-8"))
                |> Option.defaultValue response
            let contentType = response.Headers |> Map.tryFind HttpResponseHeaders.ContentType |> Option.defaultValue HttpContentTypes.Binary
            match response.Body, contentType.Split('/').[0] with
            | Text text, _ -> Text.Encoding.UTF8.GetBytes text, contentType
            | Binary _, "text" -> failwithf "Content-Typeに%sが指定されたにもかかわらずバイナリデータでした" contentType
            | Binary binary, _ -> binary, contentType
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getAnnotation (baseUri : Uri) token projectID taskID inputDataID =
        let apiUri = Uri(baseUri, sprintf "projects/%s/tasks/%s/inputs/%s/annotation" projectID taskID inputDataID)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> GetAnnotationResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getMyAccount (baseUri : Uri) token =
        let apiUri = Uri(baseUri, "accounts/my")
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> GetMyAccountResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let putAnnotation (baseUri : Uri) token projectID taskID inputDataID (request : PutAnnotationRequest.Root) =
        let apiUri = Uri(baseUri, sprintf "projects/%s/tasks/%s/inputs/%s/annotation" projectID taskID inputDataID)
        let body = request.JsonValue.ToString() |> HttpRequestBody.TextRequest
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "PUT", headers = getJsonAuthHeaders token, body = body, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> PutAnnotationResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let operateTask (baseUri : Uri) token projectID taskID (request : OperateTaskRequest.Root) =
        let apiUri = Uri(baseUri, sprintf "projects/%s/tasks/%s/operate" projectID taskID)
        let body = request.JsonValue.ToString() |> HttpRequestBody.TextRequest
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "POST", headers = getJsonAuthHeaders token, body = body, responseEncodingOverride = "UTF-8", silentHttpErrors = true)
            response, token
        let parseResponse = getTextResponse >> OperateTaskResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    let getAnnotations (baseUri : Uri) token projectID =
        let (archiveResponse, _), token = getArchiveFull baseUri token projectID
        let temporaryDir = Seq.initInfinite (fun _ ->  Path.Combine(Path.GetTempPath(), Path.GetRandomFileName())) |> Seq.find (Directory.Exists >> not)
        Directory.CreateDirectory temporaryDir |> ignore
        try
            use stream = new MemoryStream(archiveResponse)
            let zip = Zip.FastZip()
            zip.ExtractZip(stream, temporaryDir, Zip.FastZip.Overwrite.Always, null, "", "", zip.RestoreDateTimeOnExtract, true)
            let files = temporaryDir |> Directory.EnumerateDirectories |> Seq.collect Directory.EnumerateFiles
            let annotations = files |> Seq.map AnnotationData.Load |> Seq.groupBy (fun a -> a.TaskId) |> Seq.map (fun (k, v) -> k, v |> Seq.toList) |> dict
            annotations, token
        finally
            Directory.Delete(temporaryDir, false)

open Annofab

type Annofab private() =
    static member getTasks(baseUri : Uri, token, projectID, ?page : int, ?limit : int, ?phase : TaskPhase, ?status : TaskStatus, ?accountID, ?isNoUser, ?previousAccountID) =
        let queryNoUser = if isNoUser |> Option.exists id then "?no_user" else ""
        let query =
            [   "page", page |> Option.map string
                "limit", limit |> Option.map string
                "phase", phase |> Option.map string
                "status", status |> Option.map string
                "account_id", accountID
                "previous_account_id", previousAccountID
            ] |> List.choose (fun (k, vo) -> vo |> Option.map (fun v -> k, v))
        let apiUri = Uri(baseUri, sprintf "projects/%s/tasks%s" projectID queryNoUser)
        let sendApi token =
            let response = Http.Request(url = apiUri.AbsoluteUri, httpMethod = "GET", headers = getJsonAuthHeaders token, responseEncodingOverride = "UTF-8", silentHttpErrors = true, query = query)
            response, token
        let parseResponse = getTextResponse >> GetTasksResponse.Parse
        sendAndRefreshToken baseUri sendApi parseResponse token

    static member getAllTasks(baseUri : Uri, token, projectID, ?phase : TaskPhase, ?status : TaskStatus, ?accountID, ?isNoUser, ?previousAccountID) =
        let result = ResizeArray()
        let rec loop page token =
            printfn "Annofab.getAllTasks : page%dを取得" page
            let response, token = Annofab.getTasks(baseUri, token, projectID, page = page, ?phase = phase, ?status = status, ?accountID = accountID, ?isNoUser = isNoUser, ?previousAccountID = previousAccountID)
            result.AddRange(response.List)
            if response.PageNo < response.TotalPageNo then loop (page + 1) token else result |> Seq.toArray, token
        loop 1 token
