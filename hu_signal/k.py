
class K:
    def __init__(self, s):
        self.open = s.open
        self.low  = s.low
        self.high = s.high
        self.close = s.close

    def is_yang(self, ):
        if self.close > self.open:
            return True
        else:
            return False

    def is_yin(self, ):
        if self.close < self.open:
            return True
        else:
            return False

    def entity_has(self, price):
        if price > self.open and price < self.close :
            return True
        else:
            return False

    def is_logo_yang(self, ):
        #print(self.close, self.open, self.high, self.low)
        if (self.high - self.low) > 0:
            if (self.close - self.open)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def is_logo_yin(self, ):
        if (self.high - self.low) > 0:
            if (self.open - self.close)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def chg_ratio(self, pre_price):
        return round((self.close - pre_price)/pre_price,4)

    def is_long_up_tail(self,):
        if self.is_yang():
            if (self.high - self.close)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def is_long_down_tail(self,):
        if self.is_yin():
            if (self.close - self.low)/(self.high - self.low) > 0.67:
                return True
        else:
            return False

    def is_normal_up_tail(self,):
        if self.is_yang():
            if (self.high - self.close)/(self.high - self.low) > 0.5:
                return True
        else:
            return False

    def is_normal_down_tail(self,):
        if self.is_yin():
            if (self.close - self.low)/(self.high - self.low) > 0.5:
                return True
        else:
            return False
