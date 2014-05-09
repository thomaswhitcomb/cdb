%%
%% @author Tom Whitcomb
%% @doc This module implements the creation and reading of a constant database (cdb)
%% @version 1.0
%%
%% This module provides functions to create and query a CDB (constant database).
%% A CDB implements a two-level hashtable which provides fast {key,value} lookups
%% that remain fairly constant in speed regardless of the CDBs size.
%%
%% The first level in the CDB occupies the first 255 doublewords in the file.  Each
%% doubleword slot contains two values.  The first is a file pointer to the primary
%% hashtable (at the end of the file) and the second value is the number of entries 
%% in the hashtable.  The first level table of 255 entries is indexed with the lower
%% eight bits of the hash of the input key.
%%
%% Following the 255 doublewords are the {key,value} tuples.  The tuples are 
%% packed in the file without regard to word boundaries.  Each {key,value} tuple
%% is represented with a four byte key length, a four byte value length,
%% the actual key value followed by the actual value.
%%
%% Following the {key,value} tuples are the primary hash tables.  There are at most
%% 255 hash tables.  Each hash table is referenced by one of the 255 doubleword entiries
%% at the top of the file. For efficiency reasons, each hash table is allocated twice the
%% number of entries that it will need.  Each entry in the hash table is a doubleword.
%% The first word is the corresponding hash value and the second word is a file pointer
%% to the actual {key,value} tuple higher in the file.
%%

-module(cdb).

-export([from_dict/2,create/2,dump/1,get/2]).

-include_lib("eunit/include/eunit.hrl").

-define(DWORD_SIZE,8).
-define(WORD_SIZE,4).

%%
%% from_dict(FileName,ListOfKeyValueTuples)
%% Given a filename and a dictionary, create a cdb
%% using the key value pairs from the dict.
%%
%% @spec from_dict(filename(),dictionary()) -> ok
%% where
%%	filename() = string(),
%%	dictionary() = dict()
%%
from_dict(FileName,Dict) ->
  KeyValueList = dict:to_list(Dict),
  create(FileName,KeyValueList).
%%
%% to_dict(FileName)
%% Given a filename returns a dict containing
%% the key value pairs from the dict.
%%
%% @spec to_dict(filename()) -> dictionary()
%% where
%%	filename() = string(),
%%	dictionary() = dict()
%%
to_dict(FileName) ->
  KeyValueList = dump(FileName),
  dict:from_list(KeyValueList).
%%
%% create(FileName,ListOfKeyValueTuples) -> ok
%% Given a filename and a list of {key,value} tuples,
%% this function creates a CDB
%%
create(FileName,KeyValueList) ->
  {ok,Handle} = file:open(FileName,write),
  {ok,_} = file:position(Handle,{bof,2048}),
  Dict = write_key_value_pairs(Handle,KeyValueList),

  {ok,BasePos} = file:position(Handle,cur),
  L2 = write_hash_tables(Handle,Dict),

  write_top_index_table(Handle,BasePos,L2),

  file:close(Handle).

%%
%% dump(FileName) -> List
%% Given a file name, this function returns a list
%% of {key,value} tuples from the CDB.
%%
%%
%% @spec dump(filename()) -> key_value_list()
%% where
%%	filename() = string(),
%%	key_value_list() = [{key,value}]
%%
dump(FileName) ->
  {ok,Handle} = file:open(FileName,[binary,raw]),
  Fn = fun(Index,Acc) ->
    {ok,_} = file:position(Handle,{bof,?DWORD_SIZE*Index}),
    {_,Count} = read_next_2_integers(Handle),
    Acc+Count    
  end,
  NumberOfPairs = lists:foldl(Fn,0,lists:seq(0,255)) bsr 1,
  
  {ok,_} = file:position(Handle,{bof,2048}),
  Fn1 = fun(_I,Acc) ->
    {KL,VL} = read_next_2_integers(Handle),
    Key = read_next_string(Handle,KL),
    Value = read_next_string(Handle,VL),
    {ok,CurrLoc} = file:position(Handle,cur),
    Return = case get(Handle,Key) of
      {Key,Value} -> {Key,Value};
       X ->    
         %io:format("wonky: ~p->~p~n",[Key,X]),
         {wonky,X}
    end,
    {ok,_} = file:position(Handle,CurrLoc),
    [Return | Acc]
  end,
  lists:foldr(Fn1,[],lists:seq(0,NumberOfPairs-1)).
    
%%
%% get(FileName,Key) -> {key,value}
%% Given a filename and a key, returns a key and value tuple.
%%
get(FileName,Key) when is_list(FileName), is_list(Key) ->
  {ok,Handle} = file:open(FileName,[binary,raw]),
  get(Handle,Key);

get(Handle,Key) when is_tuple(Handle), is_list(Key) ->
  Hash = hash(Key),
  Index = hash_to_index(Hash),
  {ok,_} = file:position(Handle,{bof,?DWORD_SIZE*Index}),

  % Get location of hashtable and number of entries in the hash
  {HashTable,Count} = read_next_2_integers(Handle),

  case Count of
    0 ->
      undefined;
    _ ->
      {ok,HashTable} = file:position(Handle,{bof,HashTable}),

      % Get starting slot in hashtable
      Slot = hash_to_slot(Hash,Count),  
      {ok,_} = file:position(Handle,{cur,Slot*?DWORD_SIZE}),

      % Key is already bound and double checks the request and 
      % response key are the same
      LocList = lists:seq(HashTable,HashTable+((Count-1) * ?DWORD_SIZE),?DWORD_SIZE),

      % Split list around starting slot.
      {L1,L2} = lists:split(Slot,LocList),
      search_hash_table(Handle,lists:append(L2,L1),Hash,Key)
  end.

read_next_string(Handle,Length) ->
  {ok,Bin} = file:read(Handle,Length),
  binary_to_list(Bin).
   
read_next_2_integers(Handle) ->
  {ok,<<Int1:32>>} = file:read(Handle,?WORD_SIZE),
  {ok,<<Int2:32>>} = file:read(Handle,?WORD_SIZE),
  {endian_flip(Int1),endian_flip(Int2)}.

% Seach the hash table for the matching hash
% and key.  Be prepared for multiple keys
% to have the same hash value.
search_hash_table(_Handle,[],_Hash,_Key) -> missing;
search_hash_table(Handle,[Entry|RestOfEntries],Hash,Key) ->

  {ok,_} = file:position(Handle,{bof,Entry}),

  {StoredHash,DataLoc} = read_next_2_integers(Handle),

  case StoredHash of
    Hash ->

      {ok,_} = file:position(Handle,{bof,DataLoc}),
      {KeyLength,ValueLength} = read_next_2_integers(Handle),

      case read_next_string(Handle,KeyLength) of
        Key ->  % If same key as passed in, then found!
          Value = read_next_string(Handle,ValueLength),
          {Key,Value};
        _ ->
          search_hash_table(Handle,RestOfEntries,Hash,Key)
      end;
    _ ->
      search_hash_table(Handle,RestOfEntries,Hash,Key)
  end.

% Write Key and Value tuples into the CDB.  Each tuple consists of a
% 4 byte key length, a 4 byte value length, the actual key followed
% by the value.
%
% Returns a dictionary that is keyed by
% the least significant 8 bits of each hash with the
% values being a list of the hash and the position of the 
% key/value binary in the file.
write_key_value_pairs(Handle,KeyValueList) -> 

  Fn = fun({Key,_} = Pair,Dict) ->
    {ok,Pos} = file:position(Handle,{cur,0}),
    Hash = hash(Key),
    Bin = key_value_to_record(Pair), % create binary for Key and Value
    file:write(Handle,Bin),

    Index = hash_to_index(Hash), %get least signficant 8 bites from Hash

    case dict:find(Index,Dict) of
      {ok,_Prev} ->
        dict:append(Index,{Hash,Pos},Dict);
      error ->
        dict:store(Index,[{Hash,Pos}],Dict)
    end
  end,

  lists:foldl(Fn,dict:new(),KeyValueList).

% Write the actual hashtables at the 
% bottom of the file.  Each hash table
% entry is a doubleword in length.  The
% first word is the hash value corresponding
% to a key and the second word is a file
% pointer to the corresponding {key,
% value} tuple.
write_hash_tables(Handle,Dict) ->
  Fn = fun({Index,HashList},Acc) ->
    {ok,CurrPos} = file:position(Handle,{cur,0}),
    Bin = hashlist_to_bin(HashList),
    file:write(Handle,Bin),
    [{Index,CurrPos,(length(HashList)*2)}|Acc]
  end,
  List = lists:keysort(1,dict:to_list(Dict)),
  lists:foldl(Fn,[],List).

% Write the top most 255 doubleword
% entries.  First word is the 
% file pointer to a hashtable
% and the second word is the number
% of entries in the hash table
write_top_index_table(Handle,BasePos,List) ->
  Seq = lists:seq(0,255),
  Fn1 = fun(I,Acc) ->
    case lists:keysearch(I,1,List) of
      {value,Tuple} ->
        [Tuple|Acc];
      false ->
        [{I,BasePos,0}|Acc]
    end
  end,

  CompleteList = lists:keysort(1,lists:foldl(Fn1,[],Seq)),

  Fn = fun({Index,Pos,Count},CurrPos) ->
    {ok,_} = file:position(Handle,{bof,?DWORD_SIZE*Index}),
    case Count == 0 of
      true ->
        PosLE = endian_flip(CurrPos),
        NextPos = CurrPos;
      false ->
        PosLE = endian_flip(Pos),
        NextPos = Pos+(Count*?DWORD_SIZE)
    end, 
    CountLE = endian_flip(Count),
    Bin = <<PosLE:32,CountLE:32>>,
    file:write(Handle,Bin),
    NextPos
  end,

  lists:foldl(Fn,BasePos,CompleteList).
  
% HashList is a list of tuples.
% Each tuple is a Hash and a file Position of
% a key value pair.
hashlist_to_bin(HashList) ->
  Fn = fun({Hash,Pos},BinList) ->
    HashLE = endian_flip(Hash),
    PosLE = endian_flip(Pos),
    NewBin = <<HashLE:32,PosLE:32>>,

    Slot1 = find_open_slot(BinList,Hash),
    {L1,[<<0:32,0:32>>|L2]} = lists:split(Slot1,BinList),
    lists:append(L1,[NewBin|L2])
  end,
  BinList = lists:duplicate(length(HashList)*2,<<0:32,0:32>>),
  lists:foldl(Fn,BinList,HashList).

% Slot is zero based because it comes from a REM
find_open_slot(List,Hash) ->
  Len = length(List),
  Slot = hash_to_slot(Hash,Len), 
  Seq = lists:seq(1,Len),
  {CL1,CL2} = lists:split(Slot,Seq),
  {L1,L2} = lists:split(Slot,List),
  find_open_slot1(lists:append(CL2,CL1),lists:append(L2,L1)).
  

find_open_slot1([Slot|_RestOfSlots],[<<0:32,0:32>>|_RestOfEntries]) -> Slot-1;
find_open_slot1([_|RestOfSlots],[_|RestOfEntries]) -> 
  find_open_slot1(RestOfSlots,RestOfEntries).

  
endian_flip(Int) ->
  <<X:32/unsigned-little-integer>> = <<Int:32>>,
  X.

hash(Key) ->
  H = 5381,
  hash1(H,Key) band 16#FFFFFFFF.

hash1(H,[]) ->H;
hash1(H,[B|Rest]) ->
  H1 = H * 33,
  H2 = H1 bxor B,
  hash1(H2,Rest).

% Get the least significant 8 bits from the hash.
hash_to_index(Hash) ->
  Hash band 255.

hash_to_slot(Hash,L) ->
  (Hash bsr 8) rem L.

% Create a binary of the LengthKeyLengthValue
key_value_to_record({Key,Value}) ->
  L1 = endian_flip(length(Key)),
  L2 = endian_flip(length(Value)),
  LB1 = list_to_binary(Key), 
  LB2 = list_to_binary(Value), 
  <<L1:32,L2:32,LB1/binary,LB2/binary>>.

%%%%%%%%%%%%%%%%
% T E S T 
%%%%%%%%%%%%%%%  

hash_1_test() ->
  Hash = hash("key1"),
  ?assertMatch(Hash,2088047427).

hash_to_index_1_test() ->
  Hash = hash("key1"),
  Index = hash_to_index(Hash),
  ?assertMatch(Index,67).

hash_to_index_2_test() ->
  Hash = 256,
  I = hash_to_index(Hash),
  ?assertMatch(I,0).
 
hash_to_index_3_test() ->
  Hash = 268,
  I = hash_to_index(Hash),
  ?assertMatch(I,12).

hash_to_index_4_test() ->
  Hash = hash("key2"),
  Index = hash_to_index(Hash),
  ?assertMatch(Index,64).

write_key_value_pairs_1_test() ->
  {ok,Handle} = file:open("test.cdb",write),
  Dict = write_key_value_pairs(Handle,[{"key1","value1"},{"key2","value2"}]),
  Result = dict:to_list(Dict),
  ?assertMatch(Result,[{64,[{2088047424,18}]},{67,[{2088047427,0}]}]).

write_hash_tables_1_test() ->
  {ok,Handle} = file:open("test.cdb",write),
  Dict = dict:from_list([{64,[{6383014720,18}]},{67,[{6383014723,0}]}]),
  Result = write_hash_tables(Handle,Dict),
  ?assertMatch(Result,[{67,16,2},{64,0,2}]).

hashlist_to_bin_1_test() -> 
  HashList = [{6383014720,2066}],
  Bin = hashlist_to_bin(HashList),
  ?assertMatch(Bin,[<<0,0,0,0,0,0,0,0>>,<<64,19,117,124,18,8,0,0>>]).

hashlist_to_bin_2_test() -> 
  HashList = [{6383014723,2048}],
  Bin = hashlist_to_bin(HashList),
  ?assertMatch(Bin,[<<0,0,0,0,0,0,0,0>>,<<67,19,117,124,0,8,0,0>>]).

find_open_slot_1_test() ->
  List = [<<1:32,1:32>>,<<0:32,0:32>>,<<1:32,1:32>>,<<1:32,1:32>>],
  Slot = find_open_slot(List,0),
  ?assertMatch(Slot,1).

find_open_slot_2_test() ->
  List = [<<0:32,0:32>>,<<0:32,0:32>>,<<1:32,1:32>>,<<1:32,1:32>>],
  Slot = find_open_slot(List,0),
  ?assertMatch(Slot,0).

find_open_slot_3_test() ->
  List = [<<1:32,1:32>>,<<1:32,1:32>>,<<1:32,1:32>>,<<0:32,0:32>>],
  Slot = find_open_slot(List,2),
  ?assertMatch(Slot,3).

find_open_slot_4_test() ->
  List = [<<0:32,0:32>>,<<1:32,1:32>>,<<1:32,1:32>>,<<1:32,1:32>>],
  Slot = find_open_slot(List,1),
  ?assertMatch(Slot,0).

find_open_slot_5_test() ->
  List = [<<1:32,1:32>>,<<1:32,1:32>>,<<0:32,0:32>>,<<1:32,1:32>>],
  Slot = find_open_slot(List,3),
  ?assertMatch(Slot,2).

full_1_test() ->
  List1 = lists:sort([{"key1","value1"},{"key2","value2"}]),
  create("simple.cdb",lists:sort([{"key1","value1"},{"key2","value2"}])),
  List2 = lists:sort(dump("simple.cdb")),
  ?assertMatch(List1,List2).

full_2_test() ->
  List1 = lists:sort([{lists:flatten(io_lib:format("~s~p",[Prefix,Plug])),lists:flatten(io_lib:format("value~p",[Plug]))} ||  Plug <- lists:seq(1,2000),Prefix <- ["dsd","so39ds","oe9%#*(","020dkslsldclsldowlslf%$#","tiep4||","qweq"]]),
  create("full.cdb",List1),
  List2 = lists:sort(dump("full.cdb")),
  ?assertMatch(List1,List2).

from_dict_test() ->
  D = dict:new(),
  D1 = dict:store("a","b",D),
  D2 = dict:store("c","d",D1),
  cdb:from_dict("from_dict_test.cdb",D2),
  KVP = lists:sort(dump("from_dict_test.cdb")),
  D3 = lists:sort(dict:to_list(D2)),
  ?assertMatch(KVP,D3).

to_dict_test() ->
  D = dict:new(),
  D1 = dict:store("a","b",D),
  D2 = dict:store("c","d",D1),
  from_dict("from_dict_test.cdb",D2),
  Dict = to_dict("from_dict_test.cdb"),
  D3 = lists:sort(dict:to_list(D2)),
  D4 = lists:sort(dict:to_list(Dict)),
  ?assertMatch(D4,D3).
