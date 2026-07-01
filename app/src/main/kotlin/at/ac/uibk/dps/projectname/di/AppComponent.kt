package at.ac.uibk.dps.projectname.di

import dagger.Component
import javax.inject.Singleton

@Singleton @Component(modules = [AppModule::class]) interface AppComponent
