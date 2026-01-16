// Position types
export type Position = 
  | 'P' | 'C' | '1B' | '2B' | '3B' | 'SS' 
  | 'LF' | 'CF' | 'RF' | 'DH' | 'UTIL';

// Health risk types
export type HealthRisk = 'low' | 'medium' | 'high';

// Performance variance types
export type PerformanceVariance = 'low' | 'medium' | 'high';

export interface Player {
  id: string;
  name: string;
  position: Position;
  salary: number; // in millions
  attributes: {
    battingAverage: number;
    homeRuns: number;
    rbis: number;
    stolenBases: number;
    onBasePercentage: number;
    sluggingPercentage: number;
    era?: number; // for pitchers
    strikeouts?: number; // for pitchers
    wins?: number; // for pitchers
  };
  healthRisk: HealthRisk;
  performanceVariance: PerformanceVariance;
  // For similarity search
  embedding?: number[]; // UMAP/t-SNE coordinates [x, y]
  similarityScore?: number;
}

export interface PlayerCard extends Player {
  x: number; // UMAP/t-SNE x coordinate
  y: number; // UMAP/t-SNE y coordinate
  nearestNeighbors?: string[]; // IDs of 5 nearest players
}

export interface RosterSlot {
  position: Position;
  player: Player | null;
  x: number; // Position on diamond (0-1 normalized)
  y: number; // Position on diamond (0-1 normalized)
}

export interface Roster {
  id: string;
  name: string;
  slots: RosterSlot[];
  totalSalary: number;
  salaryCap: number;
}

export interface SimulationResult {
  wins: number[];
  playoffProbability: number;
  meanWins: number;
  stdDevWins: number;
}

export interface TradeComparison {
  currentPlayer: Player;
  recommendedPlayer: Player;
  winDelta: number; // positive = improvement
  salaryDelta: number; // positive = more expensive
  attributeDeltas: {
    [key: string]: number;
  };
}
