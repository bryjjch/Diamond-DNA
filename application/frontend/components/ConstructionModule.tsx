'use client';

import { useState, useMemo } from 'react';
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
  useDroppable,
} from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { Roster, Player, Position } from '@/lib/types';
import { mockRoster, mockPlayers } from '@/lib/mockData';

interface DroppableSlotProps {
  position: Position;
  player: Player | null;
}

function DroppableSlot({ position, player }: DroppableSlotProps) {
  const { setNodeRef, isOver } = useDroppable({
    id: position,
  });

  if (player) {
    return <PlayerCard player={player} position={position} />;
  }

  return (
    <div
      ref={setNodeRef}
      className={`w-24 h-32 border-2 border-dashed rounded-lg flex items-center justify-center text-xs transition-colors ${
        isOver
          ? 'border-blue-500 bg-blue-50 text-blue-600'
          : 'border-gray-300 bg-gray-50 text-gray-400'
      }`}
    >
      {position}
    </div>
  );
}

interface PlayerCardProps {
  player: Player;
  position: Position;
}

function PlayerCard({ player, position }: PlayerCardProps) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: `slot-${position}`,
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  const healthColor = {
    low: 'bg-green-500',
    medium: 'bg-yellow-500',
    high: 'bg-red-500',
  }[player.healthRisk];

  const varianceColor = {
    low: 'bg-green-500',
    medium: 'bg-yellow-500',
    high: 'bg-red-500',
  }[player.performanceVariance];

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      className="w-24 h-32 border-2 border-gray-400 rounded-lg bg-white shadow-md cursor-move hover:shadow-lg transition-shadow"
    >
      <div className="p-2 h-full flex flex-col">
        <div className="flex justify-between items-start mb-1">
          <div className="text-xs font-semibold truncate">{player.name}</div>
          <div className="flex gap-1">
            <div className={`w-2 h-2 rounded-full ${healthColor}`} title="Health Risk" />
            <div className={`w-2 h-2 rounded-full ${varianceColor}`} title="Performance Variance" />
          </div>
        </div>
        <div className="text-xs text-gray-600 mb-1">{position}</div>
        <div className="text-xs text-gray-500 mt-auto">
          ${player.salary.toFixed(1)}M
        </div>
      </div>
    </div>
  );
}

interface DraggablePlayerProps {
  player: Player;
}

function DraggablePlayer({ player }: DraggablePlayerProps) {
  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({
    id: `player-${player.id}`,
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      className="w-20 h-28 border border-gray-300 rounded bg-white shadow cursor-move hover:shadow-md p-2"
    >
      <div className="text-xs font-semibold truncate">{player.name}</div>
      <div className="text-xs text-gray-600">{player.position}</div>
      <div className="text-xs text-gray-500 mt-1">${player.salary.toFixed(1)}M</div>
    </div>
  );
}

export default function ConstructionModule() {
  const [roster, setRoster] = useState<Roster>(mockRoster);
  const [availablePlayers] = useState<Player[]>(mockPlayers.slice(0, 20));

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const totalSalary = useMemo(() => {
    return roster.slots.reduce((sum, slot) => {
      return sum + (slot.player?.salary || 0);
    }, 0);
  }, [roster]);

  const salaryPercentage = (totalSalary / roster.salaryCap) * 100;

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (!over) return;

    const activeId = active.id.toString();
    const overId = over.id.toString();

    // Check if dropping a player onto a roster slot
    if (activeId.startsWith('player-')) {
      const playerId = activeId.replace('player-', '');
      const player = availablePlayers.find(p => p.id === playerId);
      
      // Check if dropping on a position slot
      const slotPosition = roster.slots.find(s => s.position === overId)?.position;
      
      if (player && slotPosition) {
        setRoster(prev => ({
          ...prev,
          slots: prev.slots.map(slot =>
            slot.position === slotPosition
              ? { ...slot, player }
              : slot
          ),
        }));
      }
    }
    
    // Also handle moving players between slots
    if (activeId.startsWith('slot-') && overId) {
      const fromPosition = activeId.replace('slot-', '') as Position;
      const toPosition = overId as Position;
      
      const fromSlot = roster.slots.find(s => s.position === fromPosition);
      const toSlot = roster.slots.find(s => s.position === toPosition);
      
      if (fromSlot?.player && toSlot) {
        setRoster(prev => ({
          ...prev,
          slots: prev.slots.map(slot => {
            if (slot.position === fromPosition) {
              return { ...slot, player: toSlot.player };
            }
            if (slot.position === toPosition) {
              return { ...slot, player: fromSlot.player };
            }
            return slot;
          }),
        }));
      }
    }
  };

  // Diamond positions (normalized coordinates)
  const diamondPositions: Record<Position, { x: number; y: number }> = {
    P: { x: 0.5, y: 0.1 },
    C: { x: 0.5, y: 0.9 },
    '1B': { x: 0.3, y: 0.7 },
    '2B': { x: 0.4, y: 0.6 },
    '3B': { x: 0.6, y: 0.6 },
    SS: { x: 0.5, y: 0.65 },
    LF: { x: 0.2, y: 0.3 },
    CF: { x: 0.5, y: 0.2 },
    RF: { x: 0.8, y: 0.3 },
    DH: { x: 0.1, y: 0.1 },
    UTIL: { x: 0.9, y: 0.1 },
  };

  return (
    <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
      <div className="h-full flex flex-col">
        {/* Sticky Salary Cap Bar */}
        <div className="sticky top-0 z-50 bg-white border-b border-gray-200 p-4 mb-4 shadow-sm">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-2xl font-bold">Roster Construction</h2>
            <div className="text-right">
              <div className="text-sm text-gray-600">
                ${totalSalary.toFixed(1)}M / ${roster.salaryCap}M
              </div>
              <div className="text-xs text-gray-500">
                {((roster.salaryCap - totalSalary).toFixed(1))}M remaining
              </div>
            </div>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-4">
            <div
              className={`h-4 rounded-full transition-all ${
                salaryPercentage > 100
                  ? 'bg-red-500'
                  : salaryPercentage > 90
                  ? 'bg-yellow-500'
                  : 'bg-green-500'
              }`}
              style={{ width: `${Math.min(salaryPercentage, 100)}%` }}
            />
          </div>
        </div>

        <div className="flex-1 flex gap-6">
          {/* Diamond View */}
          <div className="flex-1 relative border border-gray-300 rounded-lg bg-green-50 overflow-hidden">
            <svg className="absolute inset-0 w-full h-full">
              {/* Draw baseball diamond */}
              <path
                d="M 50% 90% L 30% 70% L 50% 10% L 70% 70% Z"
                fill="none"
                stroke="#4ade80"
                strokeWidth="3"
              />
              {/* Draw outfield arc */}
              <path
                d="M 20% 30% Q 50% 5% 80% 30%"
                fill="none"
                stroke="#4ade80"
                strokeWidth="3"
              />
            </svg>

            {/* Player cards positioned on diamond */}
            <SortableContext items={roster.slots.map(s => `slot-${s.position}`)}>
              <div className="absolute inset-0">
                {roster.slots.map((slot) => {
                  const pos = diamondPositions[slot.position];
                  return (
                    <div
                      key={slot.position}
                      className="absolute"
                      style={{
                        left: `${pos.x * 100}%`,
                        top: `${pos.y * 100}%`,
                        transform: 'translate(-50%, -50%)',
                      }}
                    >
                      <DroppableSlot position={slot.position} player={slot.player} />
                    </div>
                  );
                })}
              </div>
            </SortableContext>
          </div>

          {/* Available Players Panel */}
          <div className="w-64 border border-gray-300 rounded-lg p-4 bg-gray-50">
            <h3 className="font-semibold mb-3">Available Players</h3>
            <div className="space-y-2 max-h-[600px] overflow-y-auto">
              <SortableContext items={availablePlayers.map(p => `player-${p.id}`)}>
                {availablePlayers.map((player) => (
                  <DraggablePlayer key={player.id} player={player} />
                ))}
              </SortableContext>
            </div>
          </div>
        </div>

        {/* Legend */}
        <div className="mt-4 p-4 bg-gray-50 rounded-lg">
          <div className="flex items-center gap-6 text-sm">
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 rounded-full bg-green-500" />
              <span>Low Risk</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 rounded-full bg-yellow-500" />
              <span>Medium Risk</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 rounded-full bg-red-500" />
              <span>High Risk</span>
            </div>
            <div className="ml-auto text-xs text-gray-600">
              First dot: Health Risk | Second dot: Performance Variance
            </div>
          </div>
        </div>
      </div>
    </DndContext>
  );
}
