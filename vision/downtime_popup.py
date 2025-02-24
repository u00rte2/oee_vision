def old():
    plant = system.gui.getParentWindow(event).getComponentForPath('Root Container').plant
    StartTime = system.gui.getParentWindow(event).getComponentForPath('Root Container').startTime
    EndTime = system.gui.getParentWindow(event).getComponentForPath('Root Container').endTime
    line = str(system.gui.getParentWindow(event).getComponentForPath('Root Container').line)
    LocationName = plant + '/Line' + line
    params = {'LocationName': LocationName, 'StartTime': StartTime, 'EndTime': EndTime}
    tableDataset = system.db.runNamedQuery('GMS/OEE/GetDowntimeEventAndCodes', params)
    system.gui.getParentWindow(event).getComponentForPath('Root Container.Table').data = tableDataset
    statePieDataset = GMS.OEE.getDowntimeTimePercentage(StartTime, EndTime, LocationName)
    system.gui.getParentWindow(event).getComponentForPath('Root Container.StatusPieChart').data = statePieDataset
    downData = system.gui.getParentWindow(event).getComponentForPath('Root Container.Template Repeater').templateParams
    updates = {"EndDate": EndTime, "FQPath": LocationName, "StartDate": StartTime, "refresh": True}
    newDownDS = system.dataset.updateRow(downData, 0, updates)
    system.gui.getParentWindow(event).getComponentForPath('Root Container.Template Repeater').templateParams = newDownDS
    unplannedPieDataset = GMS.OEE.getUnplannedDowntimeTimePercentage(StartTime, EndTime, LocationName)
    statePieDataset = GMS.OEE.getDowntimeTimePercentage(StartTime, EndTime, LocationName)
    system.gui.getParentWindow(event).getComponentForPath('Root Container.UnplannedPieChart').data = unplannedPieDataset
    return


