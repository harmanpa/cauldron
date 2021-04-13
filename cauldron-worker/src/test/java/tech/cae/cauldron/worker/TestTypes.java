/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tech.cae.cauldron.worker;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Collection;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.CauldronTaskTypeProvider;

/**
 *
 * @author peter
 */
@AutoService(CauldronTaskTypeProvider.class)
public class TestTypes extends CauldronTaskTypeProvider {

    @Override
    public Collection<Class<? extends CauldronTask>> getTaskTypes() {
        return Arrays.asList(AddingTask.class);
    }

}
