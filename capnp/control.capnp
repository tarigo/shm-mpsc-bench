@0xc0ffee1234567890;

struct Event {
  seq       @0 :UInt64;
  timestamp @1 :UInt64;
  source    @2 :Text;
  kind      @3 :Kind;
  payload   @4 :Data;

  enum Kind {
    info    @0;
    warn    @1;
    error   @2;
    metric  @3;
  }
}

interface Control {
  attach @0 (name :Text) -> (slots :UInt32, slotSize :UInt32);
  posted @1 (seq :UInt64) -> ();
}
