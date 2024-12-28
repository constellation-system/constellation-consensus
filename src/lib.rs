// Copyright © 2024 The Johns Hopkins Applied Physics Laboratory LLC.
//
// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.  If you
// would like to purchase a commercial license for this software, please
// contact APL’s Tech Transfer at 240-592-0817 or
// techtransfer@jhuapl.edu.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program.  If not, see
// <https://www.gnu.org/licenses/>.

//! Consensus component for the Constellation distributed systems platform.
//!
//! This crate provides an embeddable consensus component that type
//! that can be instantiated within a larger application.
#![feature(generic_const_exprs)]
#![feature(let_chains)]
#![allow(incomplete_features)]
#![allow(clippy::redundant_field_names)]
#![allow(clippy::type_complexity)]

pub mod component;
pub mod config;

mod recv;
mod state;
