import pandas as pd
from .parts.utils import * 
from model import config 
from cadCAD.engine import ExecutionMode, ExecutionContext,Executor
from cadCAD import configs

def run():
    '''
    Definition:
    Run simulation
    '''
    # Single
    exec_mode = ExecutionMode()
    local_mode_ctx = ExecutionContext(context=exec_mode.local_mode)

    simulation = Executor(exec_context=local_mode_ctx, configs=configs)
    raw_system_events, tensor_field, sessions = simulation.execute()
    # Result System Events DataFrame
    df = pd.DataFrame(raw_system_events)
    
    return df

def postprocessing(df):
    '''
    Definition:
    Refine and extract metrics from the simulation
    
    Parameters:
    df: simulation dataframe
    '''
    # subset to last substep
    df = df[df['substep'] == df.substep.max()]

    # Get the ABM results
    agent_ds = df.agents
    site_ds = df.sites
    timesteps = df.timestep
    
    # Get metrics

    ## Agent quantity
    prey_count = agent_ds.map(lambda s: sum([1 for agent in s.values() if agent['type'] == 'prey']))
    predator_count = agent_ds.map(lambda s: sum([1 for agent in s.values() if agent['type'] == 'predator']))
    omnivore_count = agent_ds.map(lambda s: sum([1 for agent in s.values() if agent['type'] == 'omnivore']))

    ## Food quantity
    food_at_sites = site_ds.map(lambda s: s.sum())
    food_at_prey = agent_ds.map(lambda s: sum([agent['food'] 
                                               for agent 
                                               in s.values() if agent['type'] == 'prey']))
    food_at_predators = agent_ds.map(lambda s: sum([agent['food'] 
                                                    for agent in s.values() 
                                                    if agent['type'] == 'predator']))
    food_at_omnivores = agent_ds.map(lambda s: sum([agent['food'] 
                                                    for agent in s.values() 
                                                    if agent['type'] == 'omnivore']))

    ## Food metrics
    median_site_food = site_ds.map(lambda s: np.median(s)) 
    median_prey_food = agent_ds.map(lambda s: np.median([agent['food'] 
                                                         for agent in s.values() 
                                                         if agent['type'] == 'prey']))
    median_predator_food = agent_ds.map(lambda s: np.median([agent['food'] 
                                                             for agent in s.values() 
                                                             if agent['type'] == 'predator']))
    median_omnivore_food = agent_ds.map(lambda s: np.median([agent['food'] 
                                                             for agent in s.values() 
                                                             if agent['type'] == 'omnivore']))

    ## Age metrics
    prey_median_age = agent_ds.map(lambda s: np.median([agent['age'] for agent in s.values() if agent['type'] == 'prey']))
    predator_median_age = agent_ds.map(lambda s: np.median([agent['age'] for agent in s.values() if agent['type'] == 'predator']))
    omnivore_median_age = agent_ds.map(lambda s: np.median([agent['age'] for agent in s.values() if agent['type'] == 'omnivore']))

    # Create an analysis dataset
    data = (pd.DataFrame({'timestep': timesteps,
                          'run': df.run,
                          'prey_count': prey_count,
                          'predator_count': predator_count,
                          'omnivore_count': omnivore_count,
                          'food_at_sites': food_at_sites,
                          'food_at_prey': food_at_prey,
                          'food_at_predators': food_at_predators,
                          'food_at_omnivores': food_at_omnivores,
                          'median_site_food': median_site_food,
                          'median_prey_food': median_prey_food,
                          'median_predator_food': median_predator_food,
                          'median_omnivore_food': median_omnivore_food,
                          'prey_median_age': prey_median_age,
                          'predator_median_age': predator_median_age,
                          'omnivore_median_age': omnivore_median_age})
           )
    
    return data