namespace java cn.edu.thssdb.rpc.thrift

struct Status {
  1: required i32 code;
  2: optional string msg;
}

struct GetTimeReq {
}

struct ConnectReq{
  1: required string username
  2: required string password
}

struct ConnectResp{
  1: required Status status
  2: required i64 sessionId
  3: required string msg
}

struct DisconnetReq{
  1: required i64 sessionId
}

struct DisconnetResp{
  1: required Status status
  2: required string msg
}

struct GetTimeResp {
  1: required string time
  2: required Status status
  3: required string msg
}

struct ExecuteStatementReq {
  1: required i64 sessionId
  2: required string statement
}

struct ExecuteStatementResp{
  1: required Status status
  2: required bool isAbort
  3: required bool hasResult
  4: optional string msg
  // only for query
  5: optional list<string> columnsList
  6: optional list<list<string>> rowList
}

struct RegisterReq {
  1: required i64 sessionId
  2: required string username
  3: required string password
}

struct RegisterResp {
  1: required Status status
  2: required string msg
}

service IService {
  GetTimeResp getTime(1: GetTimeReq req);
  ConnectResp connect(1: ConnectReq req);
  DisconnetResp disconnect(1: DisconnetReq req);
  ExecuteStatementResp executeStatement(1: ExecuteStatementReq req);
  RegisterResp userRegister(1: RegisterReq req);
}
