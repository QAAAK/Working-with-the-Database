import matplotlib as plt
import pandas as pd



def get_user_input():
    income = float(input("Enter your monthly income: "))
    expenses = {}
    while True:
        category = input("Enter expense category or 'done' to finish: ")
        if category.lower() == 'done':
            break
        amount = float(input(f"Enter amount for {category}: "))
        expenses[category] = amount
    return income, expenses


def calculate_budget(income, expenses):
    total_expenses = sum(expenses.values())
    balance = income - total_expenses
    return total_expenses, balance


def display_budget_summary(income, total_expenses, balance):
    print("\nBudget Summary:")
    print(f"Total Income: ${income}")
    print(f"Total Expenses: ${total_expenses}")
    print(f"Remaining Balance: ${balance}")
        
def plot_expenses(expenses):
    df = pd.DataFrame(list(expenses.items()), columns=['Category', 'Amount'])
    df.plot(kind='bar', x='Category', y='Amount', legend=False)
    plt.ylabel('Amount ($)')
    plt.title('Expense Distribution')
    plt.show()
    

    
   
