'use client';

import { useState } from 'react';
import DiscoveryModule from '@/components/DiscoveryModule';
import ConstructionModule from '@/components/ConstructionModule';
import AnalysisModule from '@/components/AnalysisModule';
import OptimizationModule from '@/components/OptimizationModule';

type Module = 'discovery' | 'construction' | 'analysis' | 'optimization';

export default function Home() {
  const [activeModule, setActiveModule] = useState<Module>('discovery');

  const modules: { id: Module; name: string; icon: string }[] = [
    { id: 'discovery', name: 'Discovery', icon: '🔍' },
    { id: 'construction', name: 'Construction', icon: '🏗️' },
    { id: 'analysis', name: 'Analysis', icon: '📊' },
    { id: 'optimization', name: 'Optimization', icon: '⚡' },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Navigation Header */}
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center">
              <h1 className="text-2xl font-bold text-gray-900">Diamond DNA</h1>
              <span className="ml-2 text-sm text-gray-500">Baseball Roster Builder & Analyzer</span>
            </div>
            <nav className="flex space-x-1">
              {modules.map((module) => (
                <button
                  key={module.id}
                  onClick={() => setActiveModule(module.id)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                    activeModule === module.id
                      ? 'bg-blue-600 text-white'
                      : 'text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  <span className="mr-2">{module.icon}</span>
                  {module.name}
                </button>
              ))}
            </nav>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="h-[calc(100vh-8rem)]">
          {activeModule === 'discovery' && <DiscoveryModule />}
          {activeModule === 'construction' && <ConstructionModule />}
          {activeModule === 'analysis' && <AnalysisModule />}
          {activeModule === 'optimization' && <OptimizationModule />}
        </div>
      </main>
    </div>
  );
}
