/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tech.cae.cauldron.api;

/**
 *
 * @author peter
 */
public interface CauldronStatusChangeListener {

    public void taskStatusChanged(String task, CauldronStatus status);
}
