import numpy as np
import pandas as pd

class Action:
    def __init__(self, type: str, token: str, price: float, quantity: int):
        self.type = type
        self.token = token
        self.price = price
        self.quantity = quantity

class ExampleStrategy:
    def __init__(self):
        self.open_price_criteria = [0.45,0.55]
        self.current_price_criteria = 0.9 
        self.already_applied = False
    def getAction(self, market_state):
        if self.already_applied:
            return Action(type="nothing", token="A", price=0, quantity=0)
        sum_criteria = 0
        if market_state["open_price"] >= self.open_price_criteria[0] and market_state["open_price"] <= self.open_price_criteria[1]:
            sum_criteria += 1
        if market_state["current_price"] >= self.current_price_criteria:
            sum_criteria += 1
        if sum_criteria == 2:
            self.already_applied = True
            return Action(type="buy", token="A", price=market_state["current_price"], quantity=1)
        else:
            return Action(type="nothing", token="A", price=0, quantity=0)
        
class State:
    def __init__(self, money: float):
        self.money = money
        self.shares = {}

    def applyAction(self, action: Action):
        self.money -= action.price * action.quantity
        self.shares[action.token] += action.quantity

    def getReturns(self):
        return self.money

def getAction(strategy, market_state):
    return strategy.getAction(market_state)

def backtest_strategy_on_fold(strategy, data_fold):
    T = data_fold.shape[0]
    nb_markets = data_fold.shape[1]
    state = State(money=1000)
    for t in range(T):
        for market in range(nb_markets):
            action = getAction(strategy, data_fold[t,market,:])
            state.applyAction(action)
    return state.getReturns()

def backtest_strategy(strategy, data):
    T = data.shape[0]
    N_splits = 5
    split_size = T // N_splits
    returns_folds = []
    for split in range(N_splits):
        data_fold = data[split*split_size:(split+1)*split_size]
        returns_folds.append(backtest_strategy_on_fold(strategy, data_fold))
    returns = np.mean(returns_folds, axis=0)
    std = np.std(returns_folds, axis=0)
    return returns, std